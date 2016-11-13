/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

int main (int argc, char **argv) {

  void *context;
  void *sock;

  if (argc != 2) {
    fprintf(stderr, "Usage: %s <address>\n", argv[0]);
    exit(1);
  }

  context = zmq_ctx_new ();
  if (!context) {
    fprintf(stderr, "Can't create ZeroMQ context\n");
    exit(1);
  }

  sock = zmq_socket (context, ZMQ_PUSH);
  if (!sock) {
    perror("zmq_socket");
    exit(1);
  }

  if (zmq_bind(sock, argv[1]) != 0) {
    perror("zmq_bind");
    exit(1);
  }

  while (1) {
    char line[65536];
    zmq_msg_t msg;
    int len;

    if (!fgets(line, sizeof(line), stdin)) {
      break;
    }

    len = strlen(line);
    zmq_msg_init(&msg);
    zmq_msg_init_size(&msg, len);
    memcpy(zmq_msg_data(&msg), line, len);
    zmq_msg_send(&msg, sock, ZMQ_NOBLOCK);
    zmq_msg_close(&msg);

  }
  //  We never get here but if we did, this would be how we end
  zmq_close (sock);
  zmq_ctx_destroy (context);

  return 0;
}
