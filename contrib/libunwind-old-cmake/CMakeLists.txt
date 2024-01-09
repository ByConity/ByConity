enable_language(ASM)

set(LIBUNWIND_OLD_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libunwind-old")
if (ARCH_PPC64LE)
    add_library(unwind-old
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/init.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/flush_cache.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/mempool.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/strerror.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/_ReadULEB.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/_ReadSLEB.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/backtrace.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/dyn-cancel.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/dyn-info-list.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/dyn-register.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Ldyn-extract.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lfind_dynamic_proc_info.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lget_accessors.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lget_proc_info_by_ip.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lget_proc_name.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lput_dynamic_unwind_info.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Ldestroy_addr_space.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lget_reg.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lset_reg.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lget_fpreg.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lset_fpreg.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lset_caching_policy.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/dwarf/Lexpr.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/dwarf/Lfde.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/dwarf/Lfind_proc_info-lsb.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/dwarf/Lparser.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/dwarf/Lpe.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/dwarf/global.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/elf64.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/os-linux.c

    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/is_fpreg.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/regname.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/get_func_addr.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc/Linit_local.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc/Linit_remote.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc/Lis_signal_frame.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc/longjmp.S
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc/Lreg_states_iterate.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc/siglongjmp.S
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/setcontext.S
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/Lcreate_addr_space.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/Lglobal.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/Linit.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/Lreg_states_iterate.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/Lregs.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/Lresume.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/Lstep.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/regname.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/ppc64/setcontext.S
    )
else ()
    add_library(unwind-old
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/init.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/flush_cache.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/mempool.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/strerror.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/_ReadULEB.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/_ReadSLEB.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/backtrace.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/dyn-cancel.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/dyn-info-list.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/dyn-register.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Ldyn-extract.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lfind_dynamic_proc_info.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lget_accessors.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lget_proc_info_by_ip.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lget_proc_name.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lput_dynamic_unwind_info.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Ldestroy_addr_space.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lget_reg.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lset_reg.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lget_fpreg.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lset_fpreg.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/mi/Lset_caching_policy.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/dwarf/Lexpr.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/dwarf/Lfde.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/dwarf/Lfind_proc_info-lsb.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/dwarf/Lparser.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/dwarf/Lpe.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/dwarf/global.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/elf64.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/os-linux.c

    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/is_fpreg.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/regname.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/setcontext.S
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Lcreate_addr_space.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Lget_save_loc.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Lglobal.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Linit.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Linit_local.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Linit_remote.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Lget_proc_info.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Lregs.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Lresume.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Lstash_frame.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Lstep.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Ltrace.c
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/getcontext.S
    ${LIBUNWIND_OLD_SOURCE_DIR}/src/x86_64/Los-linux.c
    )
endif()

find_file (HAVE_ATOMIC_OPS_H "atomic_ops.h")
configure_file (${LIBUNWIND_OLD_SOURCE_DIR}/config/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config/config.h)
configure_file (${LIBUNWIND_OLD_SOURCE_DIR}/config/libunwind.h.in ${CMAKE_CURRENT_BINARY_DIR}/config/libunwind.h)
configure_file (${LIBUNWIND_OLD_SOURCE_DIR}/config/libunwind-common.h.in ${CMAKE_CURRENT_BINARY_DIR}/config/libunwind-common.h)
configure_file (${LIBUNWIND_OLD_SOURCE_DIR}/config/tdep/libunwind_i.h.in ${CMAKE_CURRENT_BINARY_DIR}/config/tdep/libunwind_i.h)

target_compile_definitions (unwind-old PRIVATE HAVE_CONFIG_H=1 _XOPEN_SOURCE _GNU_SOURCE)
target_compile_options (unwind-old PRIVATE -Wno-visibility -Wno-header-guard)

target_include_directories (unwind-old PUBLIC $<BUILD_INTERFACE:${LIBUNWIND_OLD_SOURCE_DIR}/include>)
target_include_directories (unwind-old PRIVATE $<BUILD_INTERFACE:${LIBUNWIND_OLD_SOURCE_DIR}/include/tdep>)
target_include_directories (unwind-old PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/config>)
target_include_directories (unwind-old PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/config/tdep>)
target_include_directories (unwind-old PRIVATE $<BUILD_INTERFACE:${LIBUNWIND_OLD_SOURCE_DIR}/src>)

install(
        TARGETS unwind-old
        EXPORT global
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
