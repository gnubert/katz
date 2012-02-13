/*
 * Copyright (C) 2012  Ludwig Ortmann <ludwig.ortmann@fu-berlin.de>
 *
 * This file is part of katz.  
 *
 * katz is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * katz is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with katz.  If not, see <http://www.gnu.org/licenses/>.
 *
*/ 

#include <sys/time.h>
#include <time.h>

// TODO:
// - BUF_SIZE:
//   548 is the minimum UDP payload that will always come through
//   according to stevens, this is nonsense of course.
//   1) the IP header is variably long,
//   2) hardware that shitty is pretty rare.
//   -> use sensible default (1500 - pppoe - max avg ip - udp - katz)
//
// - TIMEOUT should be derived from RTT
// - MIN_SLEEP probably also needs some tuning

#define BUF_SIZE 1000 // XXX: make sure net code can deal with larger packets
#define DEFAULT_BW -1   // bw throttle in bps, -1 to disable
#define MIN_SLEEP 25    // to prevent wasting CPU for overly exact bw throttling
#define TIMEOUT 200     // when expecting data
#define KEEPALIVE 5000  // when not expecting data
#define MAXQLEN 20      // default window size



/********************************************************************** 
 ********************************************************************** 

 DATASTRUCTURES

 ********************************************************************** 
 **********************************************************************/


#define KNIL 0
#define KSYN 1
#define KACK 2
#define KFIN 3

#define KSI 0
#define KSO 1
#define KFI 2
#define KFO 3

struct katzpack {
    uint32_t seq;
    uint32_t ack;
    uint8_t  flag;
    char*    data;
};

struct katzq {
    char            *data;
    int             len;
    int             seq;
    struct timespec sent;
    struct katzq    *next;
};


struct katzconn {
    int         sock;

    // protocol state
    uint32_t    seq;
    uint32_t    ack;
    uint8_t     flag;
    int         keepalive; // how often to send packets in light of no traffic at all
    int         timeout;   // time after which a packet is deemed lost
    struct timespec
                last_event; // time of last recv/send for use with keepalive
    
    // bandwidth state
    int         bwlim;
    int         bwrate;
    time_t      last_check;

    // used to enable/disable based on protocoll state
    struct pollfd   *ipfd; 
    struct pollfd   *opfd;

    // data buffers
    struct katzq    *oq;
    struct katzq    *oq_last;
    int             n_oq;
    int             oq_maxlen; // aka winsize

    struct katzq    *iq;
    struct katzq    *iq_last;
    int             n_iq;
    int             outstanding_ack;
    // TODO: implement recv win?
};


struct katzparm {
    //char *srchost;
    char *srcport;
    char *dsthost;
    char *dstport;
    int  ai_family;
    int  keepalive;
    int  bw;
};

/**
 * Return difference between (struct timespec*) t2 and t1.
 */
#define m_tsm(t1, t2) \
    ( \
      ((t2)->tv_sec - (t1)->tv_sec)*1000 \
      + ((t2)->tv_nsec - (t1)->tv_nsec)/1000000 \
      )

const char* event_name(int event);
void print_katzq(struct katzq *q);
void print_katzpack(struct katzpack *p);
void print_katzconn(struct katzconn *conn);

inline struct katzq *free_katzq(struct katzq *q);
inline void katzconn_process_ack(struct katzconn *conn, int ack);
inline void katzconn_insert_oq(struct katzconn *conn, char *data, int len);
inline struct katzq *katzconn_getq(struct katzconn *conn);

inline int katzconn_currate (struct katzconn *conn);
inline int katzconn_allow (struct katzconn *conn);
inline int katzconn_sleep (struct katzconn *conn, int len);
inline int katzconn_keepalive(struct katzconn *conn);

int recv_katzpack(int sock, struct katzpack *p);
int send_katzpack(struct katzconn *conn, struct katzpack *p, int len);
int katz_process_out(struct katzconn *conn);
int katz_process_in(struct katzconn *conn);

int bind_socket(
        int sock, struct addrinfo *remotehints,
        char *localport);
int connected_udp_socket(struct katzparm *kp);

int katz_read(struct katzconn *conn, char *buf, int len);
int katz_write(struct katzconn *conn, char *buf, int len);

int katz_connect(struct katzconn *conn);
void katz_disconnect(struct katzconn *conn);
void katz_init_connection(
        struct katzconn *conn, struct katzparm *kp,
        struct pollfd *ipfd, struct pollfd *opfd);
void katz_peer(struct katzparm *kp);

#ifdef DEBUG
#define debug(...) fprintf(stderr,  __VA_ARGS__)
#else
#define debug(...) ((void) 0)
#endif
