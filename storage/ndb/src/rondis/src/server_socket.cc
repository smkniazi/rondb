// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <string.h>

#include "server_socket.h"
#include "pink_util.h"
#include "pink_define.h"

namespace pink {

ServerSocket::ServerSocket(int port, bool is_block)
    : port_(port),
      send_timeout_(0),
      recv_timeout_(0),
      accept_timeout_(0),
      accept_backlog_(1024),
      tcp_send_buffer_(0),
      tcp_recv_buffer_(0),
      keep_alive_(false),
      listening_(false),
  is_block_(is_block) {
}

ServerSocket::~ServerSocket() {
  Close();
}

/*
 * Listen to a specific ip addr on a multi eth machine
 * Return 0 if Listen success, other wise
 */
int ServerSocket::Listen(const std::string &bind_ip) {
  int ret = 0;
  sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
  memset(&servaddr_, 0, sizeof(servaddr_));

  int yes = 1;
  ret = setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
  if (ret < 0) {
    return kSetSockOptError;
  }

  servaddr_.sin_family = AF_INET;
  if (bind_ip.empty()) {
    servaddr_.sin_addr.s_addr = htonl(INADDR_ANY);
  } else {
    servaddr_.sin_addr.s_addr = inet_addr(bind_ip.c_str());
  }
  servaddr_.sin_port = htons(port_);

  fcntl(sockfd_, F_SETFD, fcntl(sockfd_, F_GETFD) | FD_CLOEXEC);

  ret = bind(sockfd_, (struct sockaddr *) &servaddr_, sizeof(servaddr_));
  if (ret < 0) {
    return kBindError;
  }
  ret = listen(sockfd_, accept_backlog_);
  if (ret < 0) {
    return kListenError;
  }
  listening_ = true;

  if (is_block_ == false) {
    SetNonBlock();
  }
  return kSuccess;
}

int ServerSocket::SetNonBlock() {
  flags_ = Setnonblocking(sockfd());
  if (flags_ == -1) {
    return -1;
  }
  return 0;
}

void ServerSocket::Close() {
  close(sockfd_);
}

}  // namespace pink
