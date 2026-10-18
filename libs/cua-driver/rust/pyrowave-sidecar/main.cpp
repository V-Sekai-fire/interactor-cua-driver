// cua-tracer-pyrowave: encodes the trace store's window views as PyroWave, as XR Pilot's trace store
// keeps its frames. The writer thread of cua-tracer starts one of these and keeps it for the session,
// so the GPU device and encoders are made once.
//
// Protocol on stdin/stdout, all integers little-endian u32:
//   request:  width, height, max_bytes, then width*height*4 bytes of RGBA8 (sRGB); width and height even
//   reply:    length, then length bytes: one PyroWave packet, the frame XR Pilot's decoders read.
//             length 0 means the frame could not be encoded; the session goes on.
// The colour conversion is XR Pilot's (tools/trace_roundtrip.cpp): BT.709 full range on the sRGB
// values, chroma averaged over 2x2 into 4:2:0.
//
//   cua-tracer-pyrowave                       serve the protocol
//   cua-tracer-pyrowave sweep <rgba> <w> <h>  (Vulkan) encode one RGBA8 file at several budgets,
//                                             decode each and print bytes, bits per pixel and Y PSNR
#ifdef __APPLE__
#include "pyrowave_metal.h"
#else
#include <cstdint>
#include <vulkan/vulkan_core.h> // pyrowave.h needs the Vulkan types first
#include "pyrowave.h"
#endif
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <utility>
#include <vector>
#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

namespace
{
struct Planes
{
    int width = 0;
    int height = 0;
    std::vector<uint8_t> y, cb, cr;
};

uint8_t unit(float v)
{
    return uint8_t(std::lround(std::clamp(v, 0.0f, 1.0f) * 255.0f));
}

Planes fromRgba(const uint8_t* rgba, int w, int h)
{
    Planes p;
    p.width = w;
    p.height = h;
    const size_t n = size_t(w) * size_t(h);
    p.y.resize(n);
    p.cb.resize(n / 4);
    p.cr.resize(n / 4);
    std::vector<float> cb(n), cr(n);
    for (size_t i = 0; i < n; ++i)
    {
        const float r = rgba[i * 4] / 255.0f, g = rgba[i * 4 + 1] / 255.0f, b = rgba[i * 4 + 2] / 255.0f;
        const float Y = 0.2126f * r + 0.7152f * g + 0.0722f * b;
        p.y[i] = unit(Y);
        cb[i] = (b - Y) / 1.8556f;
        cr[i] = (r - Y) / 1.5748f;
    }
    for (int y = 0; y < h / 2; ++y)
        for (int x = 0; x < w / 2; ++x)
        {
            const size_t a = size_t(2 * y) * size_t(w) + size_t(2 * x);
            const size_t o = size_t(y) * size_t(w / 2) + size_t(x);
            p.cb[o] = unit((cb[a] + cb[a + 1] + cb[a + size_t(w)] + cb[a + size_t(w) + 1]) / 4.0f + 0.5f);
            p.cr[o] = unit((cr[a] + cr[a + 1] + cr[a + size_t(w)] + cr[a + size_t(w) + 1]) / 4.0f + 0.5f);
        }
    return p;
}

pyrowave_cpu_buffer cpuBuffer(Planes& p)
{
    pyrowave_cpu_buffer b = {};
    b.data[0] = p.y.data();
    b.data[1] = p.cb.data();
    b.data[2] = p.cr.data();
    b.row_stride_in_bytes[0] = size_t(p.width);
    b.row_stride_in_bytes[1] = b.row_stride_in_bytes[2] = size_t(p.width / 2);
    b.plane_size_in_bytes[0] = p.y.size();
    b.plane_size_in_bytes[1] = p.cb.size();
    b.plane_size_in_bytes[2] = p.cr.size();
    b.width = p.width;
    b.height = p.height;
    b.format = PYROWAVE_CPU_BUFFER_FORMAT_YUV420P;
    return b;
}

struct Encoders
{
    pyrowave_device device = nullptr;
    std::map<std::pair<int, int>, pyrowave_encoder> byExtent;

    pyrowave_encoder get(int w, int h)
    {
        const auto found = byExtent.find({w, h});
        if (found != byExtent.end())
            return found->second;
        if (byExtent.size() >= 16) // windows change size; keep the set bounded
        {
            for (auto& e : byExtent)
                pyrowave_encoder_destroy(e.second);
            byExtent.clear();
        }
        pyrowave_encoder_create_info info = {};
        info.device = device;
        info.width = w;
        info.height = h;
        info.chroma = PYROWAVE_CHROMA_SUBSAMPLING_420;
        pyrowave_encoder e = nullptr;
        if (pyrowave_encoder_create(&info, &e) != PYROWAVE_SUCCESS)
            return nullptr;
        byExtent[{w, h}] = e;
        return e;
    }
};

// One packet holding the whole frame, as XR Pilot stores it; empty on failure.
std::vector<uint8_t> encode(Encoders& encoders, const uint8_t* rgba, int w, int h, size_t maxBytes)
{
    if (w < 2 || h < 2 || w > 16384 || h > 16384 || (w % 2) || (h % 2) || maxBytes == 0)
        return {};
    pyrowave_encoder e = encoders.get(w, h);
    if (!e)
        return {};
    Planes planes = fromRgba(rgba, w, h);
    const pyrowave_cpu_buffer buffer = cpuBuffer(planes);
    pyrowave_rate_control rate = {};
    rate.maximum_bitstream_size = maxBytes;
    std::vector<uint8_t> stream(maxBytes * 2 + 4096);
    const size_t boundary = stream.size();
    size_t packets = 0, written = 0;
    if (pyrowave_encoder_encode_cpu_synchronous(e, &buffer, &rate) != PYROWAVE_SUCCESS ||
        pyrowave_encoder_compute_num_packets(e, boundary, &packets) != PYROWAVE_SUCCESS || packets != 1)
        return {};
    pyrowave_packet layout = {};
    if (pyrowave_encoder_packetize(e, &layout, boundary, &written, stream.data(), stream.size()) != PYROWAVE_SUCCESS ||
        written != 1)
        return {};
    return std::vector<uint8_t>(stream.begin() + std::ptrdiff_t(layout.offset),
                                stream.begin() + std::ptrdiff_t(layout.offset + layout.size));
}

bool readAll(void* out, size_t n)
{
    return std::fread(out, 1, n, stdin) == n;
}

uint32_t le(const uint8_t* b)
{
    return uint32_t(b[0]) | uint32_t(b[1]) << 8 | uint32_t(b[2]) << 16 | uint32_t(b[3]) << 24;
}

void writeLe(uint32_t v)
{
    const uint8_t b[4] = {uint8_t(v), uint8_t(v >> 8), uint8_t(v >> 16), uint8_t(v >> 24)};
    std::fwrite(b, 1, 4, stdout);
}

int serve(Encoders& encoders)
{
    std::vector<uint8_t> rgba;
    uint8_t header[12];
    while (readAll(header, sizeof header))
    {
        const uint32_t w = le(header), h = le(header + 4), maxBytes = le(header + 8);
        if (w > 16384 || h > 16384)
            return 2; // the stream is out of step; the caller restarts the sidecar
        rgba.resize(size_t(w) * size_t(h) * 4);
        if (!readAll(rgba.data(), rgba.size()))
            return 0;
        const std::vector<uint8_t> packet = encode(encoders, rgba.data(), int(w), int(h), maxBytes);
        writeLe(uint32_t(packet.size()));
        if (!packet.empty())
            std::fwrite(packet.data(), 1, packet.size(), stdout);
        std::fflush(stdout);
    }
    return 0;
}

#ifndef __APPLE__
double psnr(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b)
{
    double sum = 0.0;
    for (size_t i = 0; i < a.size(); ++i)
        sum += double(int(a[i]) - int(b[i])) * double(int(a[i]) - int(b[i]));
    const double mse = sum / double(a.size());
    return mse <= 0.0 ? 99.0 : 10.0 * std::log10(255.0 * 255.0 / mse);
}

int sweep(Encoders& encoders, const char* path, int w, int h)
{
    FILE* f = std::fopen(path, "rb");
    if (!f)
        return std::fprintf(stderr, "cannot open %s\n", path), 1;
    std::vector<uint8_t> rgba(size_t(w) * size_t(h) * 4);
    const bool ok = std::fread(rgba.data(), 1, rgba.size(), f) == rgba.size();
    std::fclose(f);
    if (!ok)
        return std::fprintf(stderr, "%s is not %dx%d RGBA8\n", path, w, h), 1;
    const Planes reference = fromRgba(rgba.data(), w, h);
    for (double bpp : {0.25, 0.5, 1.0, 1.5, 2.0, 3.0, 4.0})
    {
        const size_t budget = size_t(double(w) * double(h) * bpp / 8.0);
        const std::vector<uint8_t> packet = encode(encoders, rgba.data(), w, h, budget);
        if (packet.empty())
        {
            std::printf("bpp %.2f: encode failed\n", bpp);
            continue;
        }
        pyrowave_decoder decoder = nullptr;
        pyrowave_decoder_create_info info = {};
        info.device = encoders.device;
        info.width = w;
        info.height = h;
        info.chroma = PYROWAVE_CHROMA_SUBSAMPLING_420;
        Planes back = reference;
        const pyrowave_cpu_buffer out = cpuBuffer(back);
        const bool decoded = pyrowave_decoder_create(&info, &decoder) == PYROWAVE_SUCCESS &&
                             pyrowave_decoder_push_packet(decoder, packet.data(), packet.size()) == PYROWAVE_SUCCESS &&
                             pyrowave_decoder_decode_is_ready(decoder, false) &&
                             pyrowave_decoder_decode_cpu_buffer_synchronous(decoder, &out) == PYROWAVE_SUCCESS;
        if (decoder)
            pyrowave_decoder_destroy(decoder);
        std::printf("bpp %.2f: %zu bytes (budget %zu), %.3f bpp actual, Y %.2f dB, Cb %.2f dB, Cr %.2f dB%s\n", bpp,
                    packet.size(), budget, packet.size() * 8.0 / (double(w) * h), decoded ? psnr(reference.y, back.y) : 0.0,
                    decoded ? psnr(reference.cb, back.cb) : 0.0, decoded ? psnr(reference.cr, back.cr) : 0.0,
                    decoded ? "" : " DECODE FAILED");
    }
    return 0;
}
#endif
} // namespace

int main(int argc, char** argv)
{
#ifdef _WIN32
    _setmode(_fileno(stdin), _O_BINARY);
    _setmode(_fileno(stdout), _O_BINARY);
#endif
    Encoders encoders;
    if (pyrowave_create_default_device(&encoders.device) != PYROWAVE_SUCCESS)
        return std::fprintf(stderr, "cua-tracer-pyrowave: no PyroWave-capable GPU device\n"), 3;
    int rc = 0;
#ifndef __APPLE__
    if (argc == 5 && std::string(argv[1]) == "sweep")
        rc = sweep(encoders, argv[2], std::atoi(argv[3]), std::atoi(argv[4]));
    else
#endif
        rc = serve(encoders);
    (void)argc;
    (void)argv;
    for (auto& e : encoders.byExtent)
        pyrowave_encoder_destroy(e.second);
    pyrowave_device_destroy(encoders.device);
    return rc;
}
