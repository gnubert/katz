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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <err.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <poll.h>
#include <assert.h>

#include "katz.h"

/********************************************************************** 
 ********************************************************************** 

 PRETTYPRINTERS

 ********************************************************************** 
 **********************************************************************/


const char* event_name(int event)
{
    switch (event) {
        case POLLOUT:
            return "POLLOUT";
        case POLLIN:
            return "POLLIN";
        case 0:
            return "NONE";
        default:
            return "OTHER";
    }
}

void print_katzq(struct katzq *q)
{
    if (q == NULL) {
        fprintf(stderr, "NULL\n");
        return;
    }
    fprintf(stderr, "data: %p\n", q->data);
    fprintf(stderr, "len: %i\n", q->len);
    fprintf(stderr, "seq: %i\n", q->seq);
    fprintf(stderr, "sent: %s\n", ctime(&q->sent.tv_sec));
    fprintf(stderr, "next: %p\n", q->next);

}

void print_katzpack(struct katzpack *p)
{
    fprintf(stderr, "====================\n");
    fprintf(stderr, "Type: ");
    switch (p->flag) {
        case KNIL:
            fprintf(stderr, "KNIL");
            break;

        case KSYN:
            fprintf(stderr, "KSYN");
            break;

        case KACK:
            fprintf(stderr, "KACK");
            break;

        case KFIN:
            fprintf(stderr, "KFIN");
            break;
    }
    fprintf(stderr, "\n");
    fprintf(stderr, "ACK: %i\n", p->ack);
    fprintf(stderr, "SEQ: %i\n", p->seq);
    fprintf(stderr, "DATA: %p\n", p->data);
    /*
    if (p->data != NULL)
        fprintf(stderr, "DATA:\n%s\n", p->data);
    else
        fprintf(stderr, "<NO DATA>\n");
    */
    fprintf(stderr, "====================\n");
}

void print_katzconn(struct katzconn *conn)
{
    fprintf(stderr, "--------------------\n");
    fprintf(stderr, "Sock: %i\n", conn->sock);
    fprintf(stderr, "Flag: ");
    switch (conn->flag) {
        case KNIL:
            fprintf(stderr, "KNIL");
            break;

        case KSYN:
            fprintf(stderr, "KSYN");
            break;

        case KACK:
            fprintf(stderr, "KACK");
            break;

        case KFIN:
            fprintf(stderr, "KFIN");
            break;
    }
    fprintf(stderr, "\n");
    fprintf(stderr, "seq: %i\n", conn->seq);
    fprintf(stderr, "ack: %i\n", conn->ack);
    fprintf(stderr, "keepalive: %i\n", conn->keepalive);
    fprintf(stderr, "timeout: %i\n", conn->timeout);
    fprintf(stderr, "last_event: %s\n", ctime(&conn->last_event.tv_sec));
    fprintf(stderr, "bwlim: %i\n", conn->bwlim);
    if (conn->bwlim != -1) {
        fprintf(stderr, "bwrate: %i\n", conn->bwrate);
        fprintf(stderr, "currate: %i\n", katzconn_currate(conn));
        fprintf(stderr, "last_check: %s\n", ctime(&conn->last_check));
    }
    fprintf(stderr, "n_iq: %i\n", conn->n_iq);
    fprintf(stderr, "n_oq: %i\n", conn->n_oq);
    fprintf(stderr, "oq_maxlen: %i\n", conn->oq_maxlen);
    fprintf(stderr, "opfd->events: %s\n", event_name(conn->opfd->events));
    fprintf(stderr, "ipfd->events: %s\n", event_name(conn->ipfd->events));
    fprintf(stderr, "--------------------\n");
}




/********************************************************************** 
 ********************************************************************** 

 KATZQ

 ********************************************************************** 
 **********************************************************************/


/**
 * Free a struct katzq,
 * return pointer to next element.
 */
inline struct katzq *free_katzq(struct katzq *q)
{
    struct katzq *p;

    p = q->next;
    free(q->data);
    free(q);

    return p;
}

/**
 * Clear acknowledged data from conn->oq.
 */
inline void katzconn_process_ack(struct katzconn *conn, int ack)
{
    debug("clearing oq(oq->seq:%i/ack:%i) ", conn->oq->seq, ack);
    while ((conn->oq != NULL) && (conn->oq->seq - ack < 0)) {
        debug(".");
        if (conn->oq_timeout == conn->oq) {
            conn->oq_timeout = conn->oq->next;
        }
        conn->seq = conn->oq->seq;
        conn->oq = free_katzq(conn->oq);
        conn->n_oq--;
    }
    if (conn->oq == NULL) {
        conn->oq_last = NULL;
    }
    debug("done.\n");
}

/**
 * Append data to connections oq.
 */
inline void katzconn_append_oq(struct katzconn *conn, char *data, int len)
{
    struct katzq *e;
    if ((e = calloc(1, sizeof(struct katzq))) == NULL)
        err(1, "calloc");

    e->data = data;
    e->len = len;
    e->next = NULL;
    e->sent.tv_sec = 0;
    e->sent.tv_nsec = 0;
    if (conn->oq == NULL) {
        conn->oq = e;
        e->seq = conn->seq+1;
    }
    else {
        e->seq = conn->oq_last->seq+1;
        conn->oq_last->next = e;
    }
    if (conn->oq_unsent == NULL) {
        conn->oq_unsent = e;
    }
    conn->oq_last = e;
    conn->n_oq++; // XXX: check for overflow
}

void grow_iq(struct katzconn *conn)
{
    struct katzq *nq, *oq;
    uint32_t new_size;
    uint32_t b1o, e1o, l1o, b1n, e1n, l1n, e2o, l2o;

#ifdef DEBUG
    uint32_t d;
    fprintf(stderr, "iq before resize: ");
    for (d = 0; d<conn->iq_size; d++) {
        fprintf(stderr, " %i", (conn->iq+d)->seq);
    }
    fprintf(stderr, "\n");
#endif

    new_size = conn->iq_size * 2; /* XXX: we can do better */
    debug("resizing iq to %i\n", new_size);

    nq = calloc(new_size, sizeof(struct katzq));
    if (nq == NULL) {
        err(1, "calloc");
    }
    oq = conn->iq;

    /*
     * Copy old q to new q in chunks.
     * The chunks could be truncated arbitarily - so we need to copy
     * and fill each chunk in (up to) two pieces.
     * old iq:      |b2o...e2o|b1o....e1o|
     * new iq: |b2n........e2n|b1n.........e1n|
     */
    b1o = (conn->ack+1) % conn->iq_size;
    e1o = conn->iq_size;
    l1o = e1o - b1o;
    e2o = (conn->ack+1)% conn->iq_size;
    l2o = e2o;

    b1n = (conn->ack+1) % new_size;
    e1n = new_size;
    l1n = e1n - b1n;

    if (l1o > l1n) {
        /* copy 1o in chunks */
        memcpy(nq+b1n, oq+b1o, l1n *sizeof(struct katzq));
        memcpy(nq, oq+b1o+l1n, (l1o-l1n) *sizeof(struct katzq));

        /* since (new_size > conn->iq_size) 2o has to fit */
        memcpy(nq+(l1o-l1n), oq, l2o *sizeof(struct katzq));
    }
    else {
        /* copy 1o in one piece */
        memcpy(nq+b1n, oq+b1o, l1o*sizeof(struct katzq));

        if (l2o > (l1n - l1o)) {
            /* copy 2o in two chunks if necessary */
            if (l2o < (l1n-l1o)) {
                /* 2o fits in rest of 1n */
                memcpy(nq+b1n+l1o, oq, l2o *sizeof(struct katzq));
            }
            else {
                /* need to chop 2o */
                memcpy(nq+b1n+l1o, oq, (l1n-l1o) *sizeof(struct katzq));
                memcpy(nq, oq+(l1n-l1o), (l2o - (l1n-l1o)) *sizeof(struct katzq));
            }
        }
        else {
            /* copy 2o in one chunk */
            memcpy(nq+b1n+l1o, oq, l2o *sizeof(struct katzq));
        }

    }

    free(conn->iq);
    conn->iq = nq;
    conn->iq_size = new_size;

#ifdef DEBUG
    fprintf(stderr, "\niq after resize: ");
    for (d = 0; d<conn->iq_size; d++) {
        fprintf(stderr, " %i", (conn->iq+d)->seq);
    }
    fprintf(stderr, "\n");
#endif
}

/**
 * Insert data with seq into conn.iq.
 * Free data if not adding.
 */
inline void katzconn_insert_iq(struct katzconn *conn, char *data, int len, int seq)
{
    struct katzq *p;

    if (seq <= conn->iq_seq_ready) {
        free(data);
        debug("got late dup packet\n");
        conn->dups++;
        return;
    }
    if (seq - conn->ack > conn->iq_size) {
        debug("growing\n");
        grow_iq(conn);
    }
    else {
        debug("not growin\n");
    }

    p = (conn->iq + (seq%conn->iq_size));
    if (p->seq != 0) {
        free(data);
        debug("got duplicate packet\n");
        conn->dups++;
        return;
    }

    /* got new packet -> ack it */
    conn->outstanding_ack = 1;

    debug("Queueing element:\n");
    p->data = data;
    p->len = len;
    p->seq = seq;
    p->next = NULL;
    p->sent.tv_sec = 0;
    p->sent.tv_nsec = 0;

#ifdef DEBUG
    //print_katzq(e);
#endif
    conn->n_iq += 1;

    /* set iq_seq_ready to highest consecutive seq number */
    int i = 0;
    while (conn->iq_seq_ready == (seq-1)) {
        i++;
        conn->iq_seq_ready = seq;
        seq += 1;
        p = (conn->iq + (seq%conn->iq_size));
        if (p->seq == 0) {
            /* hit empty iq element */
            break;
        }
        if (seq - conn->ack > conn->iq_size) {
            /* hit non-consecutive queue element
             * (XXX: this should not happen with a ringbuffer) */
            break;
        }
    }
    if (i>1) {
        conn->ooo++;
        //fprintf(stderr, "%d, ", i);
    }
    
}

/**
 * Check conn->oq for elements that need to be sent,
 * return pointer to queue element.
 * If no elements need to be sent return NULL;
 */
inline struct katzq *katzconn_getq(struct katzconn *conn)
{
    struct katzq *q;
    struct timespec now;
    
    q = conn->oq;   // current queue element

    if(clock_gettime(CLOCK_MONOTONIC, &now) == -1)
        err(1, "clock_gettime");
    

    if (conn->oq_unsent != NULL) {
        //fprintf(stderr, "n");
        q = conn->oq_unsent; /* start with unsent packets */
        conn->oq_unsent = q->next;
    }
//TODO:
//    else if (conn->lar_count > 0) {
//        fprintf(stderr, "probably need to send least seq\n");
//        exit(EXIT_FAILURE);
//    }
    else if (conn->oq_timeout != NULL ) {
        //fprintf(stderr, ",");
        //fprintf(stderr, "(ack: %d, oq: %d, oq_t: %d)", conn->seq, conn->oq->seq, conn->oq_timeout->seq);
        //fprintf(stderr, " %dp", conn->lar_count);
        q = conn->oq_timeout; /* start with first packet to timeout */
        conn->oq_timeout = NULL;
    }
    else {
        //fprintf(stderr, "e");
        q = conn->oq;
    }

    if (q != NULL) {
        clock_gettime(CLOCK_MONOTONIC, &q->sent);
    }

#ifdef DEBUG
    debug("returning oq element:\n");
    print_katzq(q);
#endif

    return q;
}


/********************************************************************** 
 ********************************************************************** 

 BWTHROTTLING

 ********************************************************************** 
 **********************************************************************/


inline int katzconn_currate (struct katzconn *conn)
{
    return (conn->bwlim - conn->bwrate);
}


/*
 * Returns number of bytes allowed to send,
 * or -1 if not limited.
 */
inline int katzconn_allow (struct katzconn *conn)
{
    time_t now;
    int time_passed;
    int rate;
    
    if (conn->bwlim == -1)
        return -1;

    now = time(NULL);
    time_passed = now - conn->last_check;
    conn->last_check = now;
    rate = conn->bwrate + time_passed * conn->bwlim;
    if (rate > conn->bwlim)
        rate = conn->bwlim;
    conn->bwrate = rate;

    return rate;
}

/*
 * Returns time in milliseconds to sleep before data of len can be sent.
 */
inline int katzconn_sleep (struct katzconn *conn, int len)
{
    int a;

    if (conn->bwlim == -1)
        return 0;
    a = katzconn_allow(conn);
    conn->bwrate -= len;
    if (len <= a) {
        return 0;
    }
    else
        return ((1.0/conn->bwlim) * 1000 * (len - a));
}


/**
 * Returns number of milliseconds to sleep at most before a keepalive
 * packet is deemed necessary.
 */
inline int katzconn_keepalive(struct katzconn *conn)
{
    struct timespec now;
    int s;
#ifdef DEBUG
    int s1, s2;
#endif

    if(clock_gettime(CLOCK_MONOTONIC, &now))
        err(1, "clock_gettime");

    //XXX: check for overflow
    s = conn->keepalive;
#ifdef DEBUG
    s1 = s;
#endif
    s += (conn->last_event.tv_sec - now.tv_sec)*1000;
#ifdef DEBUG
    s2 = s;
#endif
    s += (conn->last_event.tv_nsec/1000000 - now.tv_nsec/1000000);

    if (s < 0) {
#ifdef DEBUG
        debug("keepalive: s1: %i, s2: %i, s: %i\n", s1, s2, s);
#endif
        return 0;
    }
    else
        return s;
}


/**
 * Returns milliseconds to next timeout (connection or oq) i.e. time
 * during which sending is not reasonable.
 * Returns at least 0 (for use with poll).
 */
inline int smallest_oq_timeout (struct katzconn *conn)
{
    struct katzq *q, *l;
    struct timespec now;
    struct timespec *then;
    int t;

    if (conn->oq_unsent != NULL) {
        return 0;
    }

    if(clock_gettime(CLOCK_MONOTONIC, &now))
        err(1, "clock_gettime");

    l = NULL;
    for (q = conn->oq; q!=NULL; q=q->next) {
        if (m_tsm(&now, &q->sent) < conn->timeout) {
            if ((l == NULL) || m_tsm(&q->sent, &l->sent) > 0)
                l = q;
        }
    }

    // TODO: check for overflow
    if (l == NULL) {
        then = &conn->last_event;
    }
    else {
        then = &l->sent;
        conn->oq_timeout = l;
        debug("T:%d ", l->seq);
    }
    t = m_tsm(&now, then);
    if (l == NULL)
        t += conn->keepalive;
    else
        t += (time_t)(conn->timeout);

    if (t < 0)
        t = 0;

    debug("time to next timeout: %i\n", t);

    return t;
}

/********************************************************************** 
 ********************************************************************** 

 NETWORK COMMUNICATION

 ********************************************************************** 
 **********************************************************************/


/*
 * Receives and unmarshalls a katzpack via recvmsg.
 * Returns -1 on error, data length otherwise.
 */
int katzpack_recvmsg(int sock, struct katzpack *p)
{
    char *data;
    int nread;
    struct msghdr mh;
    struct iovec iov[2];
    
    if ((data = calloc(1, BUF_SIZE)) == NULL)
        err(1, "calloc");
    memset(&mh, 0, sizeof(mh));
    mh.msg_iovlen = 2;
    mh.msg_iov = iov;

    iov[0].iov_base = p;
    iov[0].iov_len = KPHSIZE;
    iov[1].iov_base = data;
    iov[1].iov_len = BUF_SIZE;

    p->flag = KNIL;
    if ((nread = recvmsg(sock, &mh, 0)) == -1) {
        //warn("recvmsg");
        debug("failed to receive packet\n");
        p->data = data;
        return -1;
    }
    p->data = data;
    p->seq = ntohl(p->seq);
    p->ack = ntohl(p->ack);

    nread -= KPHSIZE;

#ifdef DEBUG
    debug("received packet (datalength %i) :\n", nread);
    print_katzpack(p);
#endif

    return nread;
}

/*
 * Marshalls (inplace) and sends a katzpack via sendmsg.
 * Returns -1 on error.
 */
int katzpack_sendmsg(struct katzconn *conn, struct katzpack *p, int len)
{
    int i, delay;
    struct msghdr mh;
    struct iovec iov[2];

    //fprintf(stderr, "sending %d bytes payload\n", len);

    memset(&mh, 0, sizeof(mh));
    mh.msg_iov = iov;

    if (len == 0)
        mh.msg_iovlen = 1;
    else
        mh.msg_iovlen = 2;

    iov[0].iov_base = p;
    iov[0].iov_len = KPHSIZE;
    iov[1].iov_len = len;
    iov[1].iov_base = p->data;

    p->seq = htonl(p->seq);
    p->ack = htonl(p->ack);

    // throttle outgoing traffic
    delay = katzconn_sleep(conn, len + KPHSIZE);
    if (delay>0) {
        if (delay<MIN_SLEEP)
            delay = MIN_SLEEP;
        if (usleep(delay*1000) == -1)
            err(1, "usleep");
        else {
            debug("slept %i msecs\n", delay);
        }
    }

    i = sendmsg(conn->sock, &mh, 0);

    if (i != -1) {
        i -= KPHSIZE;
        debug("sent %i bytes\n", i);
    } else {
        debug("sendmsg failed\n");
    }

    return i;
}


int katz_process_out(struct katzconn *conn)
{
    struct katzpack p;
    struct katzq *q;
    int ret;
    int len;

    if (conn->flag != KACK)
        return -1;

    q = katzconn_getq(conn);

    /* send keepalive / acknowledge data OR send data */
    if (q==NULL) {
        debug("sending keepalive/acking data\n");
        len = 0;
        p.data = NULL;
        p.seq = 0;
    }
    else {
        debug("sending data\n");
        len = q->len;
        p.data = q->data;
        p.seq = q->seq;
    }

    p.flag = KACK;
    p.ack = conn->ack+1; // XXX: check for overflow
    // XXX: experimental - enabling this seems to trigger a bug atm:
    //p.ack = conn->iq_seq_ready+1;

    ret = katzpack_sendmsg(conn, &p, len);
    if (ret == -1)
        errx(1, "katzpack_sendmsg failed");
    else {
        if(clock_gettime(CLOCK_MONOTONIC, &conn->last_event))
            err(1, "clock_gettime");
        if (conn->n_oq == 0) {
            debug("whole queue sent, disabling KSO\n");
            conn->opfd->events = 0; // disable KSO
        }
    }
    conn->outstanding_ack = 0;

    return ret;
}

int katz_process_in(struct katzconn *conn)
{
    struct katzpack p;
    int nread;

    if (conn->flag != KACK)
        return -1;

    p.flag = KNIL;
    p.ack = 0;
    p.seq = 0;
    p.data = NULL;

    nread = katzpack_recvmsg(conn->sock, &p);
    debug("processing packet\n");
    if (nread == -1) {
        // XXX: fail if remote did not say goodbye properly?
        //errx(1, "katzpack_recvmsg");
        //conn->flag = KFIN;
        free(p.data);
        return -1;
    }

    if(clock_gettime(CLOCK_MONOTONIC, &conn->last_event))
        err(1, "clock_gettime");

    // handle irregular packets
    switch (p.flag) {
        case KSYN:
            free(p.data);
            debug("KSYN: ignoring\n");
            return 0;
        case KFIN:
            conn->flag = KFIN;
            conn->ipfd->fd = -1; // disable KSI permanently
            free(p.data);
            debug("KFIN: disable KSI\n");
            return -1;
        case KNIL: // XXX:
            errx(1, "got strange packet (KNIL) or katzpack_recvmsg failed");
        case KACK:
            debug("KACK: parsing...\n");
            break;
        default:
            errx(1, "got strange packet.");
    }


    if (p.seq > conn->iq_seq_ready) { // got new data, fill buffer
        debug("appending new data to iq\n");
        katzconn_insert_iq(conn, p.data, nread, p.seq);
    }
    else {
        free(p.data);
        debug("ignoring data, old seq\n");
    }

    if (p.ack == conn->seq+1) {
        /* got same ack again */
        conn->lar_count++;
    }
    else {
        conn->lar_count = 0;
    }
    if ((conn->oq != NULL) && (p.ack > conn->oq->seq)) {
        /* we delivered data, clear buffer */
        debug("got new ACK, clearing oq\n");
        katzconn_process_ack(conn, p.ack);
    }
    else {
        /* XXX:
         * could use a better way to decide wether SEQ was lost, maybe
         * introduce NACK
         */
        if (conn->n_oq > 0)
            conn->outstanding_ack = 1;
        else
            conn->outstanding_ack = 0;
#ifdef DEBUG
        debug("got old ACK(%i), not clearing oq:\n", p.ack);
        print_katzq(conn->oq);
#endif
    }

    return 0;
}



/********************************************************************** 
 ********************************************************************** 

 SOCKET OPERATIONS

 ********************************************************************** 
 **********************************************************************/


/*
 * Tries to bind a socket to a specific address/port tuple.
 * Returns bound socket or -1 on error
 */
int bind_socket(
        int sock, struct addrinfo *remotehints,
        char *localport)
{
    struct addrinfo hints, *res, *res0;
    int error;
#ifdef DEBUG
    char hn[NI_MAXHOST];
#endif

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = remotehints->ai_family;
    hints.ai_socktype = remotehints->ai_socktype;
    hints.ai_flags = AI_PASSIVE;
    error = getaddrinfo(NULL, localport, &hints, &res0);
    if (error)
        errx(1, "%s", gai_strerror(error));
    res = res0;

    if (bind(sock, res->ai_addr, res->ai_addrlen) != 0)
        sock = -1;

#ifdef DEBUG
    /* print address information */
    error = getnameinfo(res->ai_addr, res->ai_addrlen, hn, sizeof(hn), NULL, 0, NI_NUMERICHOST);
    if (error)
        errx(1, "%s", gai_strerror(error));
    else
        debug("(bind) res->ai_addr: %s\n", hn);

    if (res0->ai_next == NULL)
        debug("(bind) and that was the only address\n");

    for (res = res0->ai_next; res; res = res->ai_next) {
        error = getnameinfo(res->ai_addr, res->ai_addrlen, hn, sizeof(hn), NULL, 0, NI_NUMERICHOST);
        if (error)
            errx(1, "%s", gai_strerror(error));
        else
            debug("(bind ignored) res->ai_addr: %s\n", hn);
    }
#endif

    freeaddrinfo(res0);
    return sock;
}

/*
 * Returns a connected udp socket for the given parameters
 * or -1 on error.
 */
int connected_udp_socket(struct katzparm *kp)
{
    struct addrinfo hints, *res, *res0;
    int error;
    int save_errno;
    int s;
    const char *cause;
#ifdef DEBUG
    char hn[NI_MAXHOST];
#endif

    cause = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = kp->ai_family;
    hints.ai_socktype = SOCK_DGRAM;
    error = getaddrinfo(kp->dsthost, kp->dstport, &hints, &res0);
    if (error)
        errx(1, "%s", gai_strerror(error));

    s = -1;
    for (res = res0; res; res = res->ai_next) {
#ifdef DEBUG
        /* print resolved address */
        error = getnameinfo(
                res->ai_addr, res->ai_addrlen, hn, sizeof(hn), NULL, 0, NI_NUMERICHOST);
        if (error)
            errx(1, "%s", gai_strerror(error));
        else
            debug("(connect) res->ai_addr: %s\n", hn);
#endif

        /* try to create socket */
        s = socket(res->ai_family, res->ai_socktype,
                res->ai_protocol);
        if (s == -1) {
            cause = "socket";
            continue;
        }

        /* try to bind socket */
        if (bind_socket(s, &hints, kp->srcport) == -1) {
            cause = "bind";
            continue;
        }

        if (connect(s, res->ai_addr, res->ai_addrlen) == -1) {
            cause = "connect";
            save_errno = errno;
            close(s);
            errno = save_errno;
            s = -1;
            continue;
        }
  
        break;  /* okay we got one */
    }
    if (s == -1)
        err(1, cause);
    freeaddrinfo(res0);

    return s;
}




/********************************************************************** 
 ********************************************************************** 

 APPLICATION COMMUNICATION (PUBLIC API)

 ********************************************************************** 
 **********************************************************************/


/*
 * Read bytes from connection buffer.
 * Returns number of bytes read,
 * 0 on EOF,
 * -1 on error,
 * -2 if no data present.
 */
int katz_read(struct katzconn *conn, char *buf, int len)
{
    int nread;
    uint32_t next;
    struct katzq *q;

    if (conn->flag == KFIN)
        return 0;

    if (conn->flag != KACK)
        return -1;

    next = conn->ack + 1;

    q = (conn->iq + (next%conn->iq_size));
    if (q->seq == 0) {
        debug("next seq not in iq\n");
        return -2;
    }
    if (q->seq != next) {
        errx(1, "queue is broken! (internal error)\n");
    }

    if (len < q->len)
        errx(1, "not handling small reads properly atm");

    nread = q->len;
    if (nread > 0) {
        memcpy(buf, q->data, nread);
    } else {
        debug("not copying <=0 bytes from seq %i\n", q->seq);
    }

    free(q->data);
    q->seq = 0;
    conn->n_iq--;
    conn->ack++;
    conn->outstanding_ack = 1;

#ifdef DEBUG
    debug("returning %i bytes from conn->iq\n", nread);
#endif

    return nread;
}



/*
 * Write bytes from buffer to remote host.
 * Returns number of bytes written or -1 on error.
 */
int katz_write(struct katzconn *conn, char *buf, int len)
{
    if (conn->flag != KACK)
        return -1;

    if (conn->oq_maxlen > conn->n_oq) {
        // TODO: make sure len < MAX_PAYLOAD
        katzconn_append_oq(conn, buf, len);
        conn->opfd->events = POLLOUT; // enable KSO
        debug("appending %i bytes to conn->oq (now: n_oq==%i)\n", len, conn->n_oq);
        return len;
    }
    else {
        debug("not queueing, conn->n_oq(%i) == conn->oq_maxlen(%i)\n", conn->n_oq, conn->oq_maxlen);
        return 0;
    }

    return -1;
}


/*
 * Tries to establish a connection with a peer.
 * Returns -1 on error, 1 on success.
 * TODO: merge into poll loop??
 */
int katz_connect(struct katzconn *conn)
{
    int r;
    struct katzpack out, in;
    const char *cause;
    int nread;
    int custom_error;
    int MAX_RETRIES;

    MAX_RETRIES = 10;

    if (conn->flag != KNIL)
        return -1;

    custom_error = 0;
    for (r=0; r < MAX_RETRIES; r++) {
        out.seq = conn->seq;
        out.ack = conn->ack+1;
        out.flag = KSYN;
        out.data = NULL;

        debug("sending SYN\n");
        /* send SYN */
        if (katzpack_sendmsg(conn, &out, 0) == -1) {
            cause = "send";
            sleep(1);
            continue;
        }

        debug("receiving datagram\n");
        /* receive ACK */
        if ((nread = katzpack_recvmsg(conn->sock, &in)) == -1) {
            cause = "recv";
            free(in.data);
            sleep(1);
            continue;
        }
        free(in.data);

        /* got msg, check whether it is a SYN */
        if (in.flag == KSYN) {
            debug("connection established\n");
            conn->flag = KACK;
            conn->ack = in.seq;

            if (in.ack == 0) {
                debug("sending SYN reply\n");
                out.flag = KSYN;
                out.seq = conn->seq;
                out.ack = conn->ack;
                if (katzpack_sendmsg(conn, &out, 0) == -1)
                    errx(1, "katzpack_sendmsg");
            }
            else {
                debug("not sending SYN reply\n");
            }

            if(clock_gettime(CLOCK_MONOTONIC, &conn->last_event))
                err(1, "clock_gettime");

            return 1;
        }
        else {
            custom_error = 1;
            cause = "katz_protocol";
        }

    }
    if (custom_error == 0)
        err(1, cause);
    else
        errx(1, cause);

    // vvv never reached vvv

    return -1;
}

void katz_disconnect(struct katzconn *conn)
{
    struct katzpack out;

    if (conn->flag == KNIL)
        return;
    
    out.seq = conn->seq;
    out.ack = conn->ack+1;
    out.flag = KFIN;
    out.data = NULL;

    conn->flag = KFIN;
    katzpack_sendmsg(conn, &out, 0);
    katzpack_sendmsg(conn, &out, 0);
    katzpack_sendmsg(conn, &out, 0);
    // TODO: proper tear down - i.e. wait for incoming FIN?

    close(conn->sock);
    conn->sock = -1;
    conn->ack = 0;
    conn->seq = 0;
    conn->flag = KNIL;
    free(conn->iq);
}


void katz_init_connection(
        struct katzconn *conn, struct katzparm *kp,
        struct pollfd *ipfd, struct pollfd *opfd)
{
    conn->seq = 0;
    conn->ack = 0;
    conn->lar_count = 0;
    conn->flag = KNIL;
    conn->bwlim = kp->bw;
    conn->keepalive = kp->keepalive;
    conn->timeout = kp->timeout;
    conn->bwrate = 0;
    conn->last_check = time(NULL);
    conn->ipfd = ipfd;
    conn->opfd = opfd;
    conn->dups = 0;
    conn->ooo = 0;

    conn->iq = NULL;
    conn->n_iq = 0;
    conn->iq_seq_ready = 0;
    conn->outstanding_ack = 0;

    // TODO: add parameter
    conn->iq = calloc(kp->oq_maxlen, sizeof(struct katzq));
    if (conn->iq == NULL)
        err(1, "calloc");
    conn->iq_size = kp->oq_maxlen;

    conn->oq = NULL;
    conn->oq_last = NULL;
    conn->oq_timeout = NULL;
    conn->oq_unsent = NULL;
    conn->n_oq = 0;
    conn->oq_maxlen = kp->oq_maxlen;
    
    conn->sock = connected_udp_socket(kp);
    if (conn->sock == -1)
        errx(1, "connect_udp_socket");

    if (katz_connect(conn) == -1)
        exit(EXIT_FAILURE);
    else
        fprintf(stderr, "katz connected!\n");

    conn->ipfd->fd = conn->sock;
    conn->opfd->fd = conn->sock;
    conn->ipfd->events = POLLIN; // should stay that way
    conn->opfd->events = 0;      // nothing to send yet
}



/********************************************************************** 
 ********************************************************************** 

 MAIN LOOP

 ********************************************************************** 
 **********************************************************************/



void katz_peer(struct katzparm *kp)
{

    struct pollfd pfd[4];
    nfds_t nfds;
    
    int nread, nwritten;
    char obuf[BUF_SIZE], *ibuf;
    int obuflen, ibuflen;
    int timeout;

    struct katzconn conn;

    katz_init_connection(&conn, kp, &pfd[KSI], &pfd[KSO]);

    pfd[KFI].fd = STDIN_FILENO;
    pfd[KFI].events = POLLIN;
    pfd[KFO].fd = STDOUT_FILENO;
    pfd[KFO].events = 0; // will be enabled once data is available

    ibuflen = 0;
    obuflen = 0;
    //keepalive = -1;
    timeout = 0;

    while (1) {
#ifdef DEBUG
        print_katzconn(&conn);
#endif 

        if (
                ((timeout = smallest_oq_timeout(&conn)) == 0)
                || (conn.outstanding_ack != 0)) {
            pfd[KSO].events = POLLOUT;
            //fprintf(stderr, "^");
        }
        else {
            //debug("v");
            pfd[KSO].events = 0;
        }

        // - process file descriptors
        // - make sure the connection stays alive
        // keepalive = katzconn_keepalive(&conn);
        nfds = poll(pfd, 4, timeout);
        if (nfds == -1) {
            if (errno == EINTR)
                continue;
            err(1, "poll");
        }
        else if (nfds == 0) {
            debug("keepalive timeout\n");
        }
            

        // KSO
        if ((pfd[KSO].revents & POLLOUT) || (nfds == 0)) {
            katz_process_out(&conn);
        }
        // KSI
        if (pfd[KSI].revents & POLLIN) {
            katz_process_in(&conn);
            if (conn.iq_seq_ready > conn.ack)
                pfd[KFO].events = POLLOUT; // reenable KFO
            if (conn.n_oq < conn.oq_maxlen)
                pfd[KFI].events = POLLIN; // reenable KFI
        }

        // KFI
        if (pfd[KFI].revents & POLLIN) {
            if (ibuflen == 0) {
                ibuf = calloc(1, BUF_SIZE); //TODO: set to conn->MAX_PAYLOAD
                if (ibuf == NULL)
                    err(1, "calloc");
                nread = read(pfd[KFI].fd, ibuf, BUF_SIZE);
                if (nread == -1)
                    err(1, "read KFI");
                if (nread == 0) { // local EOF
                    debug("disabling KFI permanently\n");
                    pfd[KFI].fd = -1; // disable KFI permanently
                }
                else {
                    ibuflen = nread;
                    pfd[KFI].events = 0; // disable KFI
                }
            }

            nwritten = katz_write(&conn, ibuf, ibuflen);
            if (nwritten == -1)
                errx(1, "katz_write");
            else if (nwritten < ibuflen) // XXX: fix this
                errx(1, "not handling input buffer clearing properly ATM");
            else {
                ibuflen = 0;
                if (conn.n_oq < conn.oq_maxlen)
                    pfd[KFI].events = POLLIN; // reenable KFI
            }
        }

        // KFO
        if ((pfd[KFO].revents & POLLOUT) || (conn.flag == KFIN)) {
            // fill obuf if empty
            if (obuflen == 0) {
                nread = katz_read(&conn, obuf, sizeof(obuf));
                if (nread == 0) { /* remote EOF */
                    debug("quitting for remote EOF\n");
                    break; // this is safe, obuf is already empty
                } else if (nread == -1)
                    errx(1, "katz_read");
                else if (nread == -2)
                    obuflen = 0;
                else
                    obuflen = nread;
            }
            
            // clear obuf if full
            nwritten = write(pfd[KFO].fd, obuf, obuflen);
            if (nwritten == -1)
                err(1, "write KFO");
            else if (nwritten < obuflen) // XXX: fix this
                errx(1, "not handling output buffer clearing properly ATM");
            else {
                obuflen = 0;
                if (conn.iq_seq_ready == conn.ack)
                    pfd[KFO].events = 0; // disable KFO
            }
        }

        /* local EOF - outstanding buffers have been written */
        if (pfd[KFI].fd == -1 && ibuflen == 0 && conn.n_oq == 0) {
            debug("quitting for local EOF\n");
            break;
        }
        /* remote EOF - outstanding buffers have been written */
        if (pfd[KSI].fd == -1 && obuflen == 0 && conn.n_iq == 0) {
            debug("quitting for remote EOF\n");
            break;
        }

    }

    fprintf(stderr, "disconnecting...");
    fprintf(stderr, "\n(received %d duplicates, sent %d payloads, received %d payloads, >%d out of order)\n", conn.dups, conn.seq, conn.ack, conn.ooo);
    katz_disconnect(&conn);
    fprintf(stderr, "done.\n");

    return;
}





void usage(char *prgname)
{
    printf("Usage: %s [options] <local port> <remote host> <remote port>\n", prgname);
    printf("\n");
    printf("Options:\n");
    printf("  -h    print help\n");
    printf("  -4    IPv4 only mode\n");
    printf("  -6    IPv6 only mode\n");
    printf("  -l kB limit bandwidth (in kBytes/sec, -1 for unlimited)\n");
    printf("  -k s  keepalive timeout (in seconds, default %i)\n", KEEPALIVE/1000);
    printf("  -t ms acknowledgement timeout (in milliseconds, default %i)\n", TIMEOUT);
    printf("  -q n  maximum number of packets in outgoing queue (default %i)\n", MAXQLEN);
    printf("\n");
    exit(EXIT_SUCCESS);
}

int main(int argc, char **argv)
{
    int arg_index;
    struct katzparm paras;

    paras.ai_family = PF_UNSPEC;
    paras.keepalive = KEEPALIVE;
    paras.timeout = TIMEOUT;
    paras.oq_maxlen = MAXQLEN;
    paras.bw = DEFAULT_BW;

    for (arg_index = 1; arg_index < argc; arg_index++) {
        if (strcmp(argv[arg_index], "-h") == 0)
            usage(argv[0]);
        else if (strcmp(argv[arg_index], "-4") == 0)
            paras.ai_family = PF_INET;
        else if (strcmp(argv[arg_index], "-6") == 0)
            paras.ai_family = PF_INET6;
        else if (strcmp(argv[arg_index], "-l") == 0) {
            if (argc>arg_index)
                paras.bw = atoi(argv[++arg_index]) * 1000;
            else
                errx(1, "missing parameter for bandwidth");
        }
        else if (strcmp(argv[arg_index], "-k") == 0) {
            if (argc>arg_index)
                paras.keepalive = atoi(argv[++arg_index]) * 1000;
            else
                errx(1, "missing parameter for keepalive");
        }
        else if (strcmp(argv[arg_index], "-t") == 0) {
            if (argc>arg_index)
                paras.timeout = atoi(argv[++arg_index]);
            else
                errx(1, "missing parameter for timeout");
        }
        else if (strcmp(argv[arg_index], "-q") == 0) {
            if (argc>arg_index)
                paras.oq_maxlen = atoi(argv[++arg_index]);
            else
                errx(1, "missing parameter for maximum queue length");
        }
        else
            break;
    }
    if (argc - arg_index != 3) {
        fprintf(stderr, "%s: Wrong number of arguments.\nTry -h for help.\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    paras.srcport = argv[arg_index];
    paras.dsthost = argv[arg_index+1];
    paras.dstport = argv[arg_index+2];

    katz_peer(&paras);

    exit(EXIT_SUCCESS);
}
