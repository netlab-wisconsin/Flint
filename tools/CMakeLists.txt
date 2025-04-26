add_executable(ssd_tool ssd_tool.cc)
target_include_directories(ssd_tool PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(ssd_tool ${ARBITER_LIB})

add_executable(volume_tool volume_tool.cc)
target_include_directories(volume_tool PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(volume_tool ${FLINT_LIB})

add_executable(bench_tool bench_tool.cc)
target_include_directories(bench_tool PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(bench_tool ${FLINT_LIB} yaml-cpp)
