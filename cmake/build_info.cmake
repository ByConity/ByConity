add_custom_target(build_info ALL git rev-parse HEAD > build_info.txt && date >> build_info.txt)
