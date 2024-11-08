# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(VERSION_HEADER_PATH "${CMAKE_CURRENT_SOURCE_DIR}/Presto_git_build_version.h")
if(EXISTS "${VERSION_HEADER_PATH}")
    message(STATUS "The file ${VERSION_HEADER_PATH} has already been generated.")
    file(READ "${VERSION_HEADER_PATH}" VERSION_HEADER_CONTENT)
    message(STATUS "Content of ${VERSION_HEADER_PATH}:\n${VERSION_HEADER_CONTENT}")
else()
    message(STATUS "The file ${VERSION_HEADER_PATH} has not been generated. Generating now...")
    # Get commit hash
    execute_process(
        COMMAND git rev-parse HEAD
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_COMMIT_HASH
        ERROR_VARIABLE  GIT_COMMIT_ERROR
        RESULT_VARIABLE GIT_COMMIT_RESULT
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_STRIP_TRAILING_WHITESPACE
    )

    if(NOT GIT_COMMIT_RESULT EQUAL 0)
        set(GIT_COMMIT_HASH
            "error(code=${GIT_COMMIT_RESULT}): ${GIT_COMMIT_ERROR}"
        )
    endif()

    # Get branch name
    execute_process(
        COMMAND git rev-parse --abbrev-ref HEAD
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_BRANCH_NAME
        ERROR_VARIABLE  GIT_BRANCH_ERROR
        RESULT_VARIABLE GIT_BRANCH_RESULT
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_STRIP_TRAILING_WHITESPACE
    )

    if(NOT GIT_BRANCH_RESULT EQUAL 0)
        set(GIT_BRANCH_NAME
            "error(code=${GIT_BRANCH_RESULT}): ${GIT_BRANCH_ERROR}"
        )
    elseif(GIT_BRANCH_NAME STREQUAL "HEAD")
        set(GIT_BRANCH_NAME "detached-HEAD")
    endif()

    set(GENERATED_HEADER_CONTENT "/* Generated file - do not change */\n#pragma once\n#define BUILD_GIT_COMMIT \"${GIT_COMMIT_HASH}\"\n#define BUILD_GIT_BRANCH \"${GIT_BRANCH_NAME}\"\n")

    # Log the current working directory and CMAKE_SOURCE_DIR
    message(STATUS "CMAKE_CURRENT_BINARY_DIR: ${CMAKE_CURRENT_BINARY_DIR}")
    message(STATUS "CMAKE_SOURCE_DIR: ${CMAKE_SOURCE_DIR}")

    # Log the commit hash and branch name
    message(STATUS "Git commit hash: ${GIT_COMMIT_HASH}")
    message(STATUS "Git branch name: ${GIT_BRANCH_NAME}")

    # Log errors if git commands failed
    if(NOT GIT_COMMIT_RESULT EQUAL 0)
        message(WARNING "Failed to retrieve Git commit hash: ${GIT_COMMIT_ERROR}")
    endif()

    if(NOT GIT_BRANCH_RESULT EQUAL 0)
        message(WARNING "Failed to retrieve Git branch name: ${GIT_BRANCH_ERROR}")
    endif()

    # Log when writing the version header file
    message(STATUS "Writing Git version header to ${VERSION_HEADER_PATH}")
    file(WRITE "${VERSION_HEADER_PATH}" "${GENERATED_HEADER_CONTENT}")
endif()

add_library(ibm_signal_handler OBJECT IBMSignalHandler.cpp)
target_link_libraries(ibm_signal_handler presto_server_lib)


if(PRESTO_ENABLE_TESTING)
    add_subdirectory(tests)
endif()
