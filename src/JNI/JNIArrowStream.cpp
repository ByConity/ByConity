// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <errno.h>
#include <stdlib.h>

#include <stdio.h>

#include "nanoarrow/nanoarrow.h"

#include "JNIArrowStream.h"

// static struct ArrowError global_error;

// const char* my_library_last_error(void) { return ArrowErrorMessage(&global_error); }

// int print_simple_array(struct ArrowArray* array, struct ArrowSchema* schema) {
//   struct ArrowArrayView array_view;
//   NANOARROW_RETURN_NOT_OK(ArrowArrayViewInitFromSchema(&array_view, schema, &global_error));

//   char schema_str[64];
//   int len = ArrowSchemaToString(schema, schema_str, 64, 0);
//   schema_str[len] = '\0';
//   printf("schema: %s\n", schema_str);

//   int result = ArrowArrayViewSetArray(&array_view, array, &global_error);
//   if (result != NANOARROW_OK) {
//     ArrowArrayViewReset(&array_view);
//     return result;
//   }

//   for (int64_t i = 0; i < array->length; i++) {
//     printf("%d\n", (int)ArrowArrayViewGetIntUnsafe(&array_view, i));
//   }

//   ArrowArrayViewReset(&array_view);
//   return NANOARROW_OK;
// }
