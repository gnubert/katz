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
        conn->seq = conn->oq->seq;
        conn->oq = free_katzq(conn->oq);
        conn->n_oq--;
    }
    if (conn->oq == NULL)
        conn->oq_last = NULL;
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
    conn->oq_last = e;
    conn->n_oq++; // XXX: check for overflow
}

/**
 * Insert data with seq into conn.iq so that seq numbers are ordered
 * with lowest element first.
 * Free data if duplicate seq.
 */
inline void katzconn_insert_iq(struct katzconn *conn, char *data, int len, int seq)
{
    struct katzq *e, *p, *pp;
    if ((e = calloc(1, sizeof(struct katzq))) == NULL)
        err(1, "calloc");


    e->data = data;
    e->len = len;
    e->seq = seq;
    e->next = NULL;
    e->sent.tv_sec = 0;
    e->sent.tv_nsec = 0;

#ifdef DEBUG
    debug("Queueing element:\n");
    print_katzq(e);
#endif

    if (seq <= conn->ack) {
        debug("late packet");
        return;
    }
    
    pp = NULL;
    if (conn->n_iq == 0) {
        conn->n_iq = 1;
        conn->iq = e;
        conn->iq_last = e;
    }
    else {
        for (p=conn->iq; p!=NULL; p=p->next) {
            if (p->seq == seq) { // dup
                free_katzq(e);
                break;
            }
            if (p->seq < seq) { // too early in q
                pp = p;
                continue;
            }
            if (p->seq > seq) { // too far in q
                if (pp != NULL) { // not first element in q
                    pp->next = e;
                    e->next = p;
                    conn->n_iq++;
                }
                else { // first element in q
                    e->next = p;
                    conn->iq = e;
                    conn->n_iq++;
                }
                break;
            }
        }
        if (p == NULL) { // went through whole queue
            pp->next = e;
            conn->n_iq++;
        }
    }


#ifdef DEBUG
    debug("iq is now (seq): ");

    for (p = conn->iq; p!=NULL; p = p->next) {
        debug("%i, ", p->seq);
    }
    debug("\n");
#endif

}

/**
 * Check conn->oq for elements that need to be sent,
 * return pointer to queue element.
 * If no elements need to be sent return NULL;
 */
inline struct katzq *katzconn_getq(struct katzconn *conn)
{
    struct katzq *q, *e, *n, *o;
    struct timespec now;
    
    if(clock_gettime(CLOCK_MONOTONIC, &now) == -1)
        err(1, "clock_gettime");

    q = conn->oq;   // current queue element
    e = NULL;       // element with highest timeout
    n = NULL;       // first unsent element
    o = NULL;       // element least recently sent

    while (q != NULL) {
        /* not sent ever? */
        if ((q->sent.tv_sec == 0) && (q->sent.tv_nsec == 0)) {
            if (n != NULL) {
                n = q;
                break;
            }
        }
        /* timeout expired? */
        else {
            if (m_tsm(&q->sent, &now) > conn->timeout) {
                /* save first timeout element for comparison */
                if (e == NULL) {
                    e = q;
                }
                /* more so, then the last timeout we investigated? (should not happen) */
                else if (m_tsm(&e->sent, &q->sent) < 0) {
                    debug("there really was data later in the queue that timed out earlier\n");
                    e = q;
                }
            }
            if (o==NULL || (q->sent.tv_sec != 0 &&
                        ((q->sent.tv_sec < o->sent.tv_sec)
                         && (q->sent.tv_sec < o->sent.tv_nsec)))) {
                o = q;
            }
        }

        q = q->next;
    }

    
    /* timeout > new > oldest > first */
    if (e != NULL) {
        debug("(timeout)");
        fprintf(stderr, "T%i,", e->seq);
        q = e;
    }
    else if (n != NULL) {
        debug("(new)");
        fprintf(stderr, "n");
        q = n;
    }
    else if (o != NULL) {
        debug("(oldest)");
        q = o;
    }
    else {
        q = conn->oq;
    }
    if (q != NULL)
        clock_gettime(CLOCK_MONOTONIC, &q->sent);

#ifdef DEBUG
    debug("oq is now seq(sent.sec+sent.nsec): ");

    for (e = conn->oq; e!=NULL; e = e->next) {
        debug("%i(%i+%i), ", e->seq, (int)e->sent.tv_sec, (int)e->sent.tv_nsec);
    }
    debug("\n");
#endif

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

    if(clock_gettime(CLOCK_MONOTONIC, &now))
        err(1, "clock_gettime");

    l = NULL;
    for (q = conn->oq; q!=NULL; q=q->next) {

        if ((q->sent.tv_sec > 0) && (q->sent.tv_nsec > 0)) {
            if (m_tsm(&now, &q->sent) < conn->timeout) {
                if ((l == NULL) || m_tsm(&q->sent, &l->sent) < 0)
                    l = q;
            }
        }
        else { // unsent data, no time to lose!
            debug("time to next timeout: %i\n", 0);
            return 0;
        }
    }

    // TODO: check for overflow
    if (l == NULL)
        then = &conn->last_event;
    else
        then = &l->sent;
    t = m_tsm(&now, then);
    if (l == NULL)
        t += conn->keepalive;
    else
        t += (time_t)(conn->timeout);

    if (t < 0)
        t = 0;

    debug("time to next timeout: %i\n", t);

    if (t>0)
        fprintf(stderr, "t");

    return t;
}

/********************************************************************** 
 ********************************************************************** 

 NETWORK COMMUNICATION

 ********************************************************************** 
 **********************************************************************/


/*
 * Receives and unmarshalls a katzpack.
 * Returns -1 on error, data length otherwise.
 */
int recv_katzpack(int sock, struct katzpack *p)
{
    char *buf, *data;
    int off;
    int nread;
    uint32_t v;
    int vl;

    if ((buf = calloc(1, sizeof(struct katzpack) + BUF_SIZE)) == NULL)
        err(1, "calloc");
    if ((data = calloc(1, BUF_SIZE)) == NULL)
        err(1, "calloc");

    if ((nread = recv(sock, buf, sizeof(struct katzpack) + BUF_SIZE, 0)) == -1) {
        debug( "failed to receive packet\n");
        free(buf);
        return -1;
    }

    vl = sizeof(p->seq);
    memcpy(&v, buf, vl);
    p->seq = ntohl(v);
    off = vl;

    vl = sizeof(p->ack);
    memcpy(&v, buf+off, vl);
    p->ack = ntohl(v);
    off += vl;

    p->flag = buf[off];

    vl = nread - sizeof(struct katzpack);
    p->data = data;
    memcpy(p->data, buf+sizeof(struct katzpack), vl);

    //debug("<(%i,%i) ", p->seq, p->ack);

#ifdef DEBUG
    debug("received packet (datalength %i) :\n", vl);
    print_katzpack(p);
#endif
    free(buf);

    return vl;
}
/*
 * Marshalls and sends a katzpack.
 * Returns -1 on error.
 */
int send_katzpack(struct katzconn *conn, struct katzpack *p, int len)
{
    char *buf;
    int off;
    uint32_t v;
    int vl;
    int nwritten;
    int delay;

    // throttle outgoing traffic
    delay = katzconn_sleep(conn, len + sizeof(struct katzpack));
    if (delay>0) {
        if (delay<MIN_SLEEP)
            delay = MIN_SLEEP;
        if (usleep(delay*1000) == -1)
            err(1, "usleep");
        else {
            debug("slept %i msecs\n", delay);
        }
    }

    if ((buf = calloc(1, sizeof(struct katzpack) + len)) == NULL)
        err(1, "calloc");

    v = htonl(p->seq);
    vl = sizeof(p->seq);
    memcpy(buf, &v, vl);
    off = vl;

    v = htonl(p->ack);
    vl = sizeof(p->ack);
    memcpy(&buf[off], &v, vl);
    off += vl;

    buf[off] = p->flag;
    off += sizeof(p->flag);
    
    if (p->data != NULL && len != 0)
        memcpy(buf + sizeof(struct katzpack), p->data, len);

    nwritten = send(conn->sock, buf, sizeof(struct katzpack) + len, 0);
    vl = nwritten - sizeof(struct katzpack);

#ifdef DEBUG
    if (nwritten != -1)
        debug("sent packet:\n");
    else
        debug("failed sending packet:\n");
    print_katzpack(p);
#endif

    free(buf);

    if (nwritten == -1) {
        //debug(">X(%i,%i) ", p->seq, p->ack);
        return -1;
    } else {
        //debug(">(%i,%i) ", p->seq, p->ack);
        return vl;
    }
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
        debug( "sending keepalive/acking data\n");
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

    ret = send_katzpack(conn, &p, len);
    if (ret == -1)
        errx(1, "send_katzpack failed");
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

    nread = recv_katzpack(conn->sock, &p);
    debug("processing packet\n");
    if (nread == -1)
        errx(1, "recv_katzpack");

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
            errx(1, "got strange packet (KNIL) or recv_katzpack failed");
        case KACK:
            debug("KACK: parsing...\n");
            break;
        default:
            errx(1, "got strange packet.");
    }


    if (p.seq > conn->ack) { // got new data, fill buffer
        debug("appending new data to iq\n");
        katzconn_insert_iq(conn, p.data, nread, p.seq);
    }
    else {
        free(p.data);
        debug("ignoring data, old seq\n");
    }

    if ((conn->oq != NULL) && (p.ack > conn->oq->seq)) { // we delivered data, clear buffer
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

    freeaddrinfo(res);
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
    struct katzq *q;

    if (conn->flag == KFIN)
        return 0;

    if (conn->flag != KACK)
        return -1;

    if (conn->n_iq > 0) {
        q = conn->iq;
        if (len < q->len)
            errx(1, "not handling small reads properly atm");

        if (conn->iq->seq > conn->ack+1) { // only out of order data available
            return -2;
        }
        nread = q->len;
        memcpy(buf, conn->iq->data, nread);
        
        if (conn->iq_last == q)
            conn->iq_last = NULL;
        conn->iq = free_katzq(q);
        conn->n_iq--;
        conn->ack++;
        conn->outstanding_ack = 1;

#ifdef DEBUG
        debug("returning %i bytes from conn->iq which is now\n", nread);
        print_katzq(conn->iq);
#endif

        return nread;
    }
    else {
        debug("not returning bytes from conn->iq, it contains %i elements\n", conn->n_iq);
        return -2;
    }

    return -1;
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

    out.seq = 0;
    out.ack = 0;
    out.flag = KSYN;
    out.data = NULL;

    conn->flag = KSYN;
    conn->seq = out.seq;

    custom_error = 0;
    for (r=0; r < MAX_RETRIES; r++) {

        debug("sending SYN\n");
        /* send SYN */
        if (send_katzpack(conn, &out, 0) == -1) {
            cause = "send";
            sleep(1);
            continue;
        }

        debug("receiving datagram\n");
        /* receive ACK */
        if ((nread = recv_katzpack(conn->sock, &in)) == -1) {
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
                if (send_katzpack(conn, &out, 0) == -1)
                    errx(1, "send_katzpack");
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
    send_katzpack(conn, &out, 0);
    send_katzpack(conn, &out, 0);
    send_katzpack(conn, &out, 0);
    // TODO: proper tear down - i.e. wait for incoming FIN?

    close(conn->sock);
    conn->sock = -1;
    conn->ack = 0;
    conn->seq = 0;
    conn->flag = KNIL;
}


void katz_init_connection(
        struct katzconn *conn, struct katzparm *kp,
        struct pollfd *ipfd, struct pollfd *opfd)
{
    conn->seq = 0;
    conn->ack = 0;
    conn->flag = KNIL;
    conn->bwlim = kp->bw;
    conn->keepalive = kp->keepalive;
    conn->timeout = kp->timeout;
    conn->bwrate = 0;
    conn->last_check = time(NULL);
    conn->ipfd = ipfd;
    conn->opfd = opfd;

    conn->iq = NULL;
    conn->iq_last = NULL;
    conn->n_iq = 0;
    conn->outstanding_ack = 0;

    conn->oq = NULL;
    conn->oq_last = NULL;
    conn->n_oq = 0;
    conn->oq_maxlen = MAXQLEN;
    
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

        if ((timeout = smallest_oq_timeout(&conn))==0) {
            pfd[KSO].events = POLLOUT;
            //fprintf(stderr, "^");
        }
        else if (conn.outstanding_ack) {
            pfd[KSO].events = POLLOUT;
            //fprintf(stderr, "^");
        }
        else {
            //debug("v");
            //fprintf(stderr, "v");
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
            if (conn.n_iq > 0)
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
                if (nread == 0) /* remote EOF */
                    break; // this is safe, obuf is already empty
                else if (nread == -1)
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
                if (conn.n_iq == 0)
                    pfd[KFO].events = 0; // disable KFO
            }
        }

        /* local EOF - outstanding buffers have been written */
        if (pfd[KFI].fd == -1 && ibuflen == 0 && conn.n_oq == 0)
            break;
        /* remote EOF - outstanding buffers have been written */
        if (pfd[KSI].fd == -1 && obuflen == 0 && conn.n_iq == 0)
            break;

    }

    fprintf(stderr, "disconnecting...");
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
    printf("  -l BW limit bandwidth (in kBytes/sec, -1 for unlimited)\n");
    printf("  -k S  keepalive timeout (in seconds, default %i)\n", KEEPALIVE/1000);
    printf("  -t ms acknowledgement timeout (in milliseconds, default %i)\n", TIMEOUT);
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
