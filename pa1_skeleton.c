/*
# Copyright 2025 University of Kentucky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0
*/

/* 
Please specify the group members here

# Student #1: Gregory James
# Student #2:
# Student #3: 

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <pthread.h>

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define DEFAULT_CLIENT_THREADS 4

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

/*
 * This structure is used to store per-thread data in the client
 */
typedef struct {
    int epoll_fd;        /* File descriptor for the epoll instance, used for monitoring events on the socket. */
    int socket_fd;       /* File descriptor for the client socket connected to the server. */
    long long total_rtt; /* Accumulated Round-Trip Time (RTT) for all messages sent and received (in microseconds). */
    long total_messages; /* Total number of messages sent and received. */
    float request_rate;  /* Computed request rate (requests per second) based on RTT and total messages. */
} client_thread_data_t;

/*
 * This function runs in a separate client thread to handle communication with the server
 */
void *client_thread_func(void *arg) {
    client_thread_data_t *data = (client_thread_data_t *)arg;
    struct epoll_event event, events[MAX_EVENTS];
    char send_buf[MESSAGE_SIZE] = "ABCDEFGHIJKMLNOP"; /* Send 16-Bytes message every time */
    char recv_buf[MESSAGE_SIZE];
    struct timeval start, end;

    // Hint 1: register the "connected" client_thread's socket in the its epoll instance
    // Hint 2: use gettimeofday() and "struct timeval start, end" to record timestamp, which can be used to calculated RTT.

    event.events = EPOLLIN;
    event.data.fd = data->socket_fd;
    if (epoll_ctl(data->epoll_fd, EPOLL_CTL_ADD, data->socket_fd, &event) < 0) {
        perror("Epoll ctl client failed");
        exit(EXIT_FAILURE);
    }
	
    data->t_rtt = 0;
    data->t_messages = 0;



    for (int i = 0; i < num_requests; i++) {
        // Start timer
        gettimeofday(&start, NULL);

        // Send message
        if (send(data->socket_fd, send_buf, MESSAGE_SIZE, 0) < 0) {
            perror("Send failed");
            exit(EXIT_FAILURE);
        }

        // Wait for response
        int n = epoll_wait(data->epoll_fd, events, MAX_EVENTS, -1); // -1 means wait indefinitely
        if (n < 0) {
            perror("Epoll wait error");
            exit(EXIT_FAILURE);
        }

        // Read response
        int bytes_recvd = recv(data->socket_fd, recv_buf, MESSAGE_SIZE, 0);
        if (bytes_recvd <= 0) {
            perror("Recv failed or server closed");
            exit(EXIT_FAILURE);
        }

        gettimeofday(&end, NULL);

        // Calculate RTT
        long long s = end.tv_sec - start.tv_sec;
        long long micros = end.tv_usec - start.tv_usec;
        long long rtt = (s * 1000000) + micros;

        data->t_rtt += rtt;
        data->t_messages++;
    }

    // Calculate Request Rate
    double total_time_sec = (double)data->total_rtt / 1000000.0;
    if (total_time_sec > 0) {
        data->request_rate = data->t_messages / total_time_sec;
    } else {
        data->request_rate = 0.0f;
    }

    return NULL;
}

/*
 * This function orchestrates multiple client threads to send requests to a server,
 * collect performance data of each threads, and compute aggregated metrics of all threads.
 */
void run_client() {
    pthread_t threads[num_client_threads];
    client_thread_data_t thread_data[num_client_threads];
    struct sockaddr_in server_addr;

    /* TODO:
     * Create sockets and epoll instances for client threads
     * and connect these sockets of client threads to the server
     */
    
    // Configure server address
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);

    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        perror("Invalid address/ Address not supported");
        exit(EXIT_FAILURE);
    }
m
    for (int i = 0; i < num_client_threads; i++) {
        // Create TCP Socket
        if ((thread_data[i].socket_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
            perror("Socket creation error");
            exit(EXIT_FAILURE);
        }

        // Connect to Server
        if (connect(thread_data[i].socket_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            perror("Connection Failed");
            exit(EXIT_FAILURE);
        }

        // Create Epoll Instance
        if ((thread_data[i].epoll_fd = epoll_create1(0)) < 0) {
            perror("Epoll creation error");
            exit(EXIT_FAILURE);
        }
    }

    // Hint: use thread_data to save the created socket and epoll instance for each thread
    // You will pass the thread_data to pthread_create() as below
    for (int i = 0; i < num_client_threads; i++) {
        pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]);
    }

    /* TODO:
     * Wait for client threads to complete and aggregate metrics of all client threads
     */

    long long t_rtt = 0;
    long t_messages = 0;
    float t_request_rate = 0.0;

    for (int i = 0; i < num_client_threads; i++) {
        pthread_join(threads[i], NULL);

        total_rtt += thread_data[i].total_rtt;
        total_messages += thread_data[i].total_messages;
        total_request_rate += thread_data[i].request_rate;

        // Cleanup
        close(thread_data[i].socket_fd);
        close(thread_data[i].epoll_fd);
    }

    printf("Average RTT: %lld us\n", total_rtt / total_messages);
    printf("Total Request Rate: %f messages/s\n", total_request_rate);
}

void run_server() {
    int listen_fd, epoll_fd;
    struct sockaddr_in server_addr;
    struct epoll_event event, events[MAX_EVENTS];

    /* TODO:
     * Server creates listening socket and epoll instance.
     * Server registers the listening socket to epoll
     */

    // Create listening socket
    if ((listen_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Bind socket to IP and Port
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(server_ip);
    server_addr.sin_port = htons(server_port);

    if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(listen_fd, 10) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    // Create epoll instance
    if ((epoll_fd = epoll_create1(0)) < 0) {
        perror("Epoll create failed");
        exit(EXIT_FAILURE);
    }

    // Register listening socket
    event.events = EPOLLIN;
    event.data.fd = listen_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &event) < 0) {
        perror("Epoll ctl failed");
        exit(EXIT_FAILURE);
    }

    printf("Server running on %s:%d\n", server_ip, server_port);

    /* Server's run-to-completion event loop */
    while (1) {
        /* TODO:
         * Server uses epoll to handle connection establishment with clients
         * or receive the message from clients and echo the message back
         */

	int n_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (n_events < 0) {
            perror("Epoll wait failed");
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < n_events; i++) {
            // Case 1
            if (events[i].data.fd == listen_fd) {
                int conn_fd;
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);

                if ((conn_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_len)) < 0) {
                    perror("Accept failed");
                    continue;
                }

                event.events = EPOLLIN;
                event.data.fd = conn_fd;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_fd, &event) < 0) {
                    perror("Epoll add client failed");
                    close(conn_fd);
                }
            } 
            // Case 2
            else {
                int client_fd = events[i].data.fd;
                char buffer[MESSAGE_SIZE];

                int bytes_read = recv(client_fd, buffer, MESSAGE_SIZE, 0);

                if (bytes_read <= 0) {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, NULL);
                    close(client_fd);
                } else {
                    send(client_fd, buffer, bytes_read, 0);
                }
            }
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc > 1 && strcmp(argv[1], "server") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);

        run_server();
    } else if (argc > 1 && strcmp(argv[1], "client") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);
        if (argc > 4) num_client_threads = atoi(argv[4]);
        if (argc > 5) num_requests = atoi(argv[5]);

        run_client();
    } else {
        printf("Usage: %s <server|client> [server_ip server_port num_client_threads num_requests]\n", argv[0]);
    }

    return 0;
}
