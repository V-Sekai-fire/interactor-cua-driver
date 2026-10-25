# cua-tracer-pyrowave: the PyroWave encoder cua-tracer's trace store runs as a sidecar (main.cpp).
# PyroWave (MIT) and the Granite subset it needs are pinned as in XR Pilot's oxrsys/cmake/PyroWave.cmake.
# macOS builds PyroWave's Metal port; Windows and Linux build the Vulkan library, which loads Vulkan
# at run time through volk, so no Vulkan SDK is needed to build.
cmake_minimum_required(VERSION 3.20)
project(cua-tracer-pyrowave LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)
include(FetchContent)

FetchContent_Declare(
    pyrowave
    GIT_REPOSITORY https://github.com/V-Sekai-fire/pyrowave.git
    GIT_TAG 2a0db384be579e0f92639fc30259a6f8d3dc4eab
    SOURCE_SUBDIR none
)

if(APPLE)
    enable_language(OBJCXX)
    FetchContent_MakeAvailable(pyrowave)
    set(M ${pyrowave_SOURCE_DIR}/metal)
    add_library(pyrowave_metal STATIC
        ${M}/pyrowave_common.mm ${M}/pyrowave_decoder.mm ${M}/pyrowave_encoder.mm ${M}/pyrowave_bitstream.cpp)
    target_include_directories(pyrowave_metal PUBLIC ${M})
    target_compile_options(pyrowave_metal PRIVATE $<$<COMPILE_LANGUAGE:OBJCXX>:-fobjc-arc>)
    find_library(IOSURFACE_LIBRARY IOSurface REQUIRED)
    find_library(METAL_LIBRARY Metal REQUIRED)
    find_library(FOUNDATION_LIBRARY Foundation REQUIRED)
    target_link_libraries(pyrowave_metal PUBLIC ${IOSURFACE_LIBRARY} ${METAL_LIBRARY} ${FOUNDATION_LIBRARY} objc)
    add_executable(cua-tracer-pyrowave main.cpp)
    target_link_libraries(cua-tracer-pyrowave PRIVATE pyrowave_metal)
else()
    FetchContent_Declare(
        granite
        GIT_REPOSITORY https://github.com/Themaister/Granite.git
        GIT_TAG 1b2d1801d2910fb09ebcded2f0bb3a3a781103b5
        GIT_SUBMODULES third_party/volk third_party/khronos/vulkan-headers
        SOURCE_SUBDIR none
    )
    FetchContent_MakeAvailable(pyrowave granite)
    set(GRANITE_VULKAN_SHADER_MANAGER_RUNTIME_COMPILER OFF CACHE BOOL "" FORCE)
    set(GRANITE_SHADER_COMPILER_OPTIMIZE OFF CACHE BOOL "" FORCE)
    set(GRANITE_POSITION_INDEPENDENT ON CACHE BOOL "" FORCE)
    set(GRANITE_SHIPPING ON CACHE BOOL "" FORCE)
    set(GRANITE_VULKAN_SPIRV_CROSS OFF CACHE BOOL "" FORCE)
    set(GRANITE_VULKAN_SYSTEM_HANDLES OFF CACHE BOOL "" FORCE)
    set(GRANITE_RENDERER OFF CACHE BOOL "" FORCE)
    set(GRANITE_VULKAN_FOSSILIZE OFF CACHE BOOL "" FORCE)
    set(GRANITE_PLATFORM "null" CACHE STRING "" FORCE)
    add_subdirectory(${granite_SOURCE_DIR} ${CMAKE_BINARY_DIR}/_deps/granite-build EXCLUDE_FROM_ALL)
    add_library(pyrowave_vk STATIC
        ${pyrowave_SOURCE_DIR}/pyrowave_encoder.cpp
        ${pyrowave_SOURCE_DIR}/pyrowave_decoder.cpp
        ${pyrowave_SOURCE_DIR}/pyrowave_common.cpp
        ${pyrowave_SOURCE_DIR}/pyrowave_c.cpp
        ${granite_SOURCE_DIR}/video/scaler.cpp)
    target_include_directories(pyrowave_vk
        PUBLIC ${pyrowave_SOURCE_DIR}
        PRIVATE ${pyrowave_SOURCE_DIR}/shaders ${granite_SOURCE_DIR}/video)
    target_link_libraries(pyrowave_vk PUBLIC granite-vulkan granite-math)
    target_compile_definitions(pyrowave_vk PRIVATE PYROWAVE_PRECISION=1)
    add_executable(cua-tracer-pyrowave main.cpp)
    target_link_libraries(cua-tracer-pyrowave PRIVATE pyrowave_vk)
    if(MINGW)
        target_link_options(cua-tracer-pyrowave PRIVATE -static -static-libgcc -static-libstdc++)
    endif()
endif()
