// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "pink_thread.h"
#include "pink_thread_name.h"
#include "debug.h"
#include "pink_define.h"

namespace pink {

Thread::Thread()
  : should_stop_(false),
    running_(false),
    thread_id_(0) {
}

Thread::~Thread() {
}

void* Thread::RunThread(void *arg) {
  Thread* thread = reinterpret_cast<Thread*>(arg);
  if (!(thread->thread_name().empty())) {
    SetThreadName(pthread_self(), thread->thread_name());
  }
  thread->ThreadMain();
  return nullptr;
}

int Thread::StartThread() {
  std::lock_guard<std::mutex> guard(running_mu_);
  should_stop_ = false;
  if (!running_) {
    running_ = true;
    return pthread_create(&thread_id_, nullptr, RunThread, (void *)this);
  }
  return 0;
}

int Thread::StopThread() {
  std::lock_guard<std::mutex> guard(running_mu_);
  should_stop_ = true;
  if (running_) {
    running_ = false;
    return pthread_join(thread_id_, nullptr);
  }
  return 0;
}

int Thread::JoinThread() {
  return pthread_join(thread_id_, nullptr);
}

}  // namespace pink
