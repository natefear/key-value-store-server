/* Server program for key-value store. */

#include "kv.h"
#include "parser.h"
#include "parser.c"
#include "kv.c"
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h> 
#include <pthread.h>
#include <poll.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>
#include <unistd.h>
#include <errno.h>

#define NTHREADS 4
#define BACKLOG 10
#define MAXEVENTS 63

/* Add anything you want here. */
struct availableWorkList{//linked list 
    int fdsID;//id of available work
    struct availableWorkList *next;//ptr to next element of list
};

struct availableWorkList *head = NULL;//start of linked list

pthread_mutex_t workLock = PTHREAD_MUTEX_INITIALIZER;//lock for linked list
pthread_mutex_t kvLock = PTHREAD_MUTEX_INITIALIZER;//lock for kv
pthread_mutex_t conLock = PTHREAD_MUTEX_INITIALIZER;//lock for num cons
pthread_mutex_t sdLock = PTHREAD_MUTEX_INITIALIZER;//lock for final shutdown
pthread_mutex_t tsdLock = PTHREAD_MUTEX_INITIALIZER;//lock for scheduled shutdown
pthread_cond_t waitForWork = PTHREAD_COND_INITIALIZER;//work condition variables 

int availableWork = 0;//list size
int numcons = 0; //cons
int sdown = 0; //flag to wake threads and shutdown
int tsdown = 0;//flag for shutdown from final client using close cmd
int pipefd[2];//pipe to wake epoll

/* A worker thread. You should write the code of this function. */
void* worker_thread(void *arg) {
    int workerID = *(int *)arg, err, readerr = 0, fds, count, numitems, res, con = 0, lsdown = 0, ltsdown = 0;
    char buf[255], ebuf[260], *value, *key, *text;
    enum DATA_CMD d_cmd;

    for(;;) {

        err = pthread_mutex_lock(&workLock);//lock list
        if(err){printf("Worker %d lock failed\n",workerID); abort();}//catch error

        while(availableWork < 1) {//not enough work wait

            err = pthread_mutex_lock(&conLock);//lock list
            if(err){printf("Con lock failed\n"); abort();}//catch error
            con = numcons; //chekc num cons
            err = pthread_mutex_unlock(&conLock);//lock list
            if(err){printf("Con unlock failed\n"); abort();}//catch error

            err = pthread_mutex_lock(&sdLock);//lock list
            if(err){printf("Sd lock failed\n"); abort();}//catch error
            lsdown = sdown; //check if thread should exit when clients disconnect
            err = pthread_mutex_unlock(&sdLock);//lock list
            if(err){printf("Sd lock failed\n"); abort();}//catch error

            if(con < 1 && lsdown == 1){break;}//exit loop for shutdown

            err = pthread_cond_wait(&waitForWork, &workLock);//wait for work
            if(err){printf("Worker %d wait failed\n",workerID); abort();}//catch error
        }
        if(lsdown == 0){//if not final shutdown
            struct availableWorkList *tmp = head;//save current as tmp before point ahead
            fds = head->fdsID;//save id locally
            head = head->next;//point to next list item
            free(tmp);//delete current work fds
            availableWork--;//decrement num available
        }
      
        err = pthread_mutex_unlock(&workLock);//lock list
        if(err){printf("Worker %d unlock failed\n",workerID); abort();}//catch error

        if(con < 1 && lsdown == 1){printf("Worker %d exited\n",workerID); return 0;}//exit thread for shutdown
       
        printf("Worker %d has fds %d \n", workerID,fds);//print worker fds pair

        for(;;) {//read till no data left
            count = read (fds, buf, sizeof buf);//read
            if (count == 0 || errno == EAGAIN || errno == EWOULDBLOCK) { break; } //end of read //non blocking fd so egain etc valid
            else if(count == -1){ readerr = 1; break;}//flag read error
        }
        if (readerr == 1){ strerror_r(errno,ebuf,260); printf("%s\n",ebuf); continue;}//read error worker return to wait

        err = parse_d(buf,&d_cmd,&key,&text);
        if (err < 0 || err > 3) { printf("parse failed worker thread %d\n",workerID); close(fds); continue; }
        //if err return worker to waiting
             
        switch(d_cmd) {
            case D_PUT ://put
                err = pthread_mutex_lock(&kvLock);//lock list
                if(err){printf("Worker %d kv lock failed\n",workerID); abort();}//catch error
   
                value = malloc(strlen(text)+1);//add value to heap
                if(value == NULL){ printf("Worker %d kv malloc out of memory\n",workerID); abort(); }//catch error
                strcpy(value,text);//copy text to value

                res = itemExists(key);
                if(!res) {
                    err = createItem(key, value); 
                }
                else if (res) { 
                    err = updateItem(key, value);
                }
                 
                err = pthread_mutex_unlock(&kvLock);//lock list
                if(err){printf("Worker %d kv unlock failed\n",workerID); abort();}//catch error 
                    
                if(err == 0) {
                    err = write (fds, "Stored Key Sucessful.\n", 23);//write 
                    if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
                }
                else if(err == -1) {
                    err = write (fds, "Error Storing Key, Store May Be Full.\n", 41);//write 
                    if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
                }

            break;
            case D_GET ://get
                err = pthread_mutex_lock(&kvLock);//lock list
                if(err){printf("Worker %d kv lock failed\n",workerID); abort();}//catch error
                   
                res = itemExists(key);
                if (res) {
                    value = findValue(key);
                }
                else {
                    value = NULL;
                } 
               
                err = pthread_mutex_unlock(&kvLock);//lock list
                if(err){printf("Worker %d kv unlock failed\n",workerID); abort();}//catch error 
                   
                if(value != NULL){
                    char tmp[260];
                    snprintf(tmp, strlen(value)+1, "%s", value); 
                    strcat(tmp,"\n");
                 
                    err = write(fds,tmp,strlen(tmp)+ 1); 
                    if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
                }
                else if(value == NULL) {
                    err = write (fds, "Key Not Found, Key Does Not Exist.\n", 36);//write 
                    if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
                }
                  
            break;
            case D_COUNT ://count
                err = pthread_mutex_lock(&kvLock);//lock list
                if(err){printf("Worker %d kv lock failed\n",workerID); abort();}//catch error

                numitems = countItems();
                      
                err = pthread_mutex_unlock(&kvLock);//lock list
                if(err){printf("Worker %d kv unlock failed\n",workerID); abort();}//catch error
                      
                char tmp1[10];
                snprintf(tmp1, sizeof numitems, "%d", numitems); 
                strcat(tmp1,"\n");  

                err = write (fds, tmp1, strlen(tmp1));//write 
                if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }

            break;
            case D_DELETE ://delete
                err = pthread_mutex_lock(&kvLock);//lock list
                if(err){printf("Worker %d kv lock failed\n",workerID); abort();}//catch error
                   
                res = deleteItem(key, 1);
                    
                err = pthread_mutex_unlock(&kvLock);//lock list
                if(err){printf("Worker %d kv unlock failed\n",workerID); abort();}//catch error
                    
                if(res == 0) {
                    err = write (fds, "Key Sucessfully Deleted.\n", 26);//write 
                    if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
                }
                if(res == -1) { 
                    err = write (fds, "Key Not Deleted, Key Does Not Exist.\n", 38);//write 
                    if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
                }

            break;
            case D_EXISTS ://exists
                err = pthread_mutex_lock(&kvLock);//lock list
                if(err){printf("Worker %d kv lock failed\n",workerID); abort();}//catch error
                    
                res = itemExists(key);
		    
                err = pthread_mutex_unlock(&kvLock);//lock list
                if(err){printf("Worker %d kv unlock failed\n",workerID); abort();}//catch error
                   
                if(res) {
                    err = write(fds,"1\n",3);
                    if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
                }
                else if(!res) {
                    err = write(fds,"0\n",3);
                    if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
                }

            break;
            case D_END ://end
                err = write (fds, "Goodbye.\n", 10);//write 
                if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
               
                err = pthread_mutex_lock(&conLock);//lock list
                if(err){printf("Con lock failed\n"); abort();}//catch error
                numcons--; //decrease number of connections
                con = numcons;
                err = pthread_mutex_unlock(&conLock);//lock list
                if(err){printf("Con lock failed\n"); abort();}//catch error
               
                err = pthread_mutex_lock(&tsdLock);//lock list
                if(err){printf("Sd lock failed\n"); abort();}//catch error
                ltsdown = tsdown;
                err = pthread_mutex_unlock(&tsdLock);//lock list
                if(err){printf("Sd lock failed\n"); abort();}//catch error
            
                if(con > 0){ printf("Client closed connection\n");}
                shutdown(fds,SHUT_RDWR);
                close(fds);//close client fd
            
                if(ltsdown == 1 && con == 0) {
                    write(pipefd[1], "exit\n", 6);
                    shutdown(pipefd[1],SHUT_WR);
                    close(pipefd[1]);
                }
                           
            break;
            case D_ERR_OL ://over long
                err = write (fds, "Data Error: The Line Is Too Long.\n", 35);//write 
                if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
            break;
            case D_ERR_INVALID ://invalid
                err = write (fds, "Data Error: Invalid Command.\n", 30);//write 
                if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
            break;
            case D_ERR_SHORT ://short
                err = write (fds, "Data Error: Too Few Parameters.\n", 33);//write 
                if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
            break;
            case D_ERR_LONG ://long
                err = write (fds, "Data Error: Too Many Parameters.\n", 34);//write 
                if (err == -1) { printf("client write failed worker thread %d\n",workerID); close(fds); }
            break;
        }     
    }  
    return 0;    
}  
 
int create_bind (int port) {

    struct sockaddr_in sa;
    int sfd, err;

    sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0){ printf("Failed to create socket\n"); exit(0); }
    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &(int){ 1 }, sizeof(int)) < 0){
        printf("Socket option reuse address failed\n"); exit(0);
    }

    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_ANY); // or INADDR_LOOPBACK
    sa.sin_port = htons(port);

    err = bind(sfd, (struct sockaddr *) &sa, sizeof(sa));
    if (err < 0) { printf("Failed to bind to port\n"); exit(0); }

    return sfd;

}

/* You may add code to the main() function. */
int main(int argc, char** argv) {
    pthread_t workerT[NTHREADS];
    int err, cfd, dfd, nfd, csock, dsock, workerID[NTHREADS], count, efd, readerr = 0;
    int i, controlport[MAXEVENTS], numitems, lsdown = 0, con;
    char buf[255], ebuf[260];
    enum CONTROL_CMD c_cmd;
            
    struct epoll_event event, events[MAXEVENTS];

    if (argc < 3) {
        printf("Usage: %s control-port data-port\n", argv[0]);
        exit(1);
    }
    else {
        cfd = create_bind(atoi(argv[1]));
        dfd = create_bind(atoi(argv[2]));
     
        if (pipe(pipefd) == -1) {// create pipe
            printf("failed to create pipe\n");
            exit(0);
        }
        
        err = fcntl(cfd, F_SETFL, fcntl(cfd, F_GETFL, 0) | O_NONBLOCK);//make socket non blocking
        if (err == -1) { printf("Failed to non block on control port\n"); exit(0); }
        err = fcntl(dfd, F_SETFL, fcntl(dfd, F_GETFL, 0) | O_NONBLOCK);//make socket non blocking
        if (err == -1) { printf("Failed to non block on data port\n"); exit(0); }
        err = fcntl(pipefd[0], F_SETFL, fcntl(pipefd[0], F_GETFL, 0) | O_NONBLOCK);//make pipe non blocking
        if (err == -1) { printf("Failed to non block on control port\n"); exit(0); }

        for(i = 0; i< NTHREADS; i++) {
            workerID[i] = i;//set thread id
            err = pthread_create(&workerT[i], NULL, worker_thread, &workerID[i]);//create thread
            if(err){printf("worker thread  %d create failed\n",i); abort();}//catch error
        }
      
        err = listen(cfd, BACKLOG);
        if (err == -1) { printf("Failed to listen on control port\n"); exit(0); }
        err = listen(dfd, BACKLOG);
        if (err == -1) { printf("Failed to listen on data port\n"); exit(0); }
    }

    efd = epoll_create1 (0); //create epoll
    if (efd == -1) { printf("Failed to create epoll instance\n"); exit(0); }
       
    event.events = EPOLLIN | EPOLLET;//edge triggered
   
    event.data.fd = pipefd[0]; //add read pipe
    err = epoll_ctl (efd, EPOLL_CTL_ADD, pipefd[0], &event); 
    if (err == -1) { printf("Failed to register read pipe to epoll\n"); exit(0); }
   
    event.data.fd = cfd; //add control
    err = epoll_ctl (efd, EPOLL_CTL_ADD, cfd, &event);
    if (err == -1) { printf("Failed to register cfd to epoll\n"); exit(0); }
   
    event.data.fd = dfd; //add data port
    err = epoll_ctl (efd, EPOLL_CTL_ADD, dfd, &event);
    if (err == -1) { printf("Failed to register dfd to epoll\n"); exit(0); }
   
    for(;;) {

        err = pthread_mutex_lock(&conLock);//lock list
        if(err){printf("Con lock failed\n"); abort();}//catch error
        con = numcons; //check num cons
        err = pthread_mutex_unlock(&conLock);//lock list
        if(err){printf("Con unlock failed\n"); abort();}//catch error
      
        if(lsdown == 1 && con < 1 ){
            break;//final client exited terminal after shutdown has been signalled exit loop
        }

        nfd = epoll_wait(efd, events, MAXEVENTS, -1); //wait for new data
        if (nfd == -1) { printf("Failed to epoll wait\n"); exit(0); }

        for (i = 0; i < nfd; ++i) { //loop fds with data to read
        
            if(events[i].events & EPOLLHUP){//client exited terminal or final client closed by command
                printf("Client closed connection\n");
          
                err = pthread_mutex_lock(&conLock);//lock list
                if(err){printf("Con lock failed\n"); abort();}//catch error
                numcons--;//decrement num cons
                con = numcons;
                err = pthread_mutex_unlock(&conLock);//lock list
                if(err){printf("Con lock failed\n"); abort();}//catch error
            
                shutdown(events[i].data.fd,SHUT_RDWR);
                close(events[i].data.fd);
                continue;//back to loop beginning
            }        
            else if ((events[i].events & EPOLLERR) || (!(events[i].events & EPOLLIN))) {//error caused wake up
                printf("Woken Unexpectedly\n");
                shutdown(events[i].data.fd,SHUT_RDWR);
                close (events[i].data.fd);
                continue;//back to loop
            }
            else if ( events[i].data.fd == dfd || events[i].data.fd == cfd ) { //new connection
            
                if ( events[i].data.fd == dfd && lsdown == 0) {//data port con
               
                    for (;;) {
                  
                        dsock = accept(dfd, NULL, NULL);//accept new con
                        if (dsock == -1) { 
                            if ((errno == EAGAIN) || (errno == EWOULDBLOCK)){ 
                                break; 
                            } 
                            else { 
                                printf("Failed to accept on data port\n");
                                close(dsock);
                                break;
                            } 
                        }
                        printf("Accepted connection on dfd descriptor %d\n",dsock); 

                        err = pthread_mutex_lock(&conLock);//lock list
                        if(err){printf("Con lock failed\n"); abort();}//catch error
                        numcons++; //increment num cons
                        con = numcons;
                        err = pthread_mutex_unlock(&conLock);//lock list
                        if(err){printf("Con lock failed\n"); abort();}//catch error

                        err = write (dsock, "Welcome to the KV store\n", 25);//write 
                        if (err == -1) { printf("client write failed main thread \n"); close(dsock); break; } 

                        controlport[dsock] = 0;

                        err = fcntl(dsock, F_SETFL, fcntl(dsock, F_GETFL, 0) | O_NONBLOCK);//make socket non blocking
                        if (err == -1) { printf("Failed to non block on data port\n"); close(dsock); break; }

                        event.events = EPOLLIN | EPOLLET; //edge trig
                        event.data.fd = dsock; //monitor new fd

                        err = epoll_ctl (efd, EPOLL_CTL_ADD, dsock, &event);
                        if (err == -1) { printf("Failed to register new dfd to epoll\n"); close(dsock); break; }
                    }   
                }      
                else if ( events[i].data.fd == cfd ) { //control port
               
                    for (;;) {
                  
                        csock = accept(cfd, NULL, NULL);
                        if (csock == -1) { 
                            if ((errno == EAGAIN) || (errno == EWOULDBLOCK)){ 
                                break; 
                            } 
                            else { 
                                printf("Failed to accept on control port"); 
                                close(csock); 
                                break; 
                            } 
                        }
                        printf("Accepted connection on cfd descriptor %d\n",csock);

                        err = write (csock, "Welcome to the KV store\n", 25);//write 
                        if (err == -1) { printf("client write failed main thread \n"); close(csock); break; } 
                 
                        controlport[csock] = 1;

                        err = fcntl(csock, F_SETFL, fcntl(csock, F_GETFL, 0) | O_NONBLOCK);//make socket non blocking
                        if (err == -1) { printf("Failed to non block on control port\n"); close(csock); break; }

                        event.events = EPOLLIN | EPOLLET;//edge
                        event.data.fd = csock;

                        err = epoll_ctl (efd, EPOLL_CTL_ADD, csock, &event);//monitor new fd
                        if (err == -1) { printf("Failed to register new cfd to epoll\n"); close(csock); break; }
                    }
                }
                continue;
            }
            else if(controlport[events[i].data.fd] == 0){ //if the fd was accepted on the data port  
                //add fd to queue
                err = pthread_mutex_lock(&workLock);//lock avail list
                if(err){printf("Main thread work lock failed\n"); abort();}//catch error
             
                //allocate mem
                struct availableWorkList *link = (struct availableWorkList*) malloc(sizeof(struct availableWorkList));
                link->fdsID = events[i].data.fd;//add work fd to list
                link->next = head;//point new marker to previous list item
                head = link;//update head to the new item/marker
                availableWork++;//keep track of list size
        
                //signal worker
                err = pthread_cond_signal(&waitForWork);//signal any waiting workers
                if(err){printf("Main thread signal failed\n"); abort();}//catch error
	     
                err = pthread_mutex_unlock(&workLock);//lock avail list
                if(err){printf("Main thread work unlock failed\n"); abort();}//catch error
            }
            else if (controlport[events[i].data.fd] == 1){ //if the fd was accepted on the control port
           
                for(;;) {  
                    count = read (events[i].data.fd, buf, sizeof buf);//read
                    if (count == 0 || errno == EAGAIN || errno == EWOULDBLOCK) { break; } //end of read //non blocking fd so egain etc valid
                    else if(count == -1){ readerr = 1; break;}
                }
                if (readerr == 1){ strerror_r(errno,ebuf,260); printf("%s\n",ebuf); continue;}
           
                c_cmd = parse_c(buf);
                if (c_cmd < 0 || c_cmd > 2) { printf("parse failed main thread\n"); close(events[i].data.fd); continue; }

                switch(c_cmd) {
                    case C_SHUTDOWN ://shutdown  
                        err = write (events[i].data.fd, "Shutting Down.\n", 16);//write 
                        if (err == -1) { printf("client write failed main thread\n"); }
                        printf("Recieved shutdown signal, waiting for clients to disconnect\n");
                  
                        err = pthread_mutex_lock(&tsdLock);//lock list
                        if(err){printf("TSD lock failed\n"); abort();}//catch error
                        tsdown = 1;//shutdown scheduled
                        err = pthread_mutex_unlock(&tsdLock);//lock list
                        if(err){printf("TSD unlock failed\n"); abort();}//catch error
                 
                        lsdown = 1;
                        shutdown(dfd,SHUT_RDWR);
                        close(dfd);
                        dfd = -30;//set to impossible fd value                              
                    break;
                    case C_COUNT ://count
                        err = pthread_mutex_lock(&kvLock);//lock list
                        if(err){printf("Maint thread kv lock failed\n"); abort();}//catch error

                        numitems = countItems();
                      
                        err = pthread_mutex_unlock(&kvLock);//lock list
                        if(err){printf("Main thread kv unlock failed\n"); abort();}//catch error
                      
                        char tmp2[10];//?
                        snprintf(tmp2, sizeof numitems, "%d", numitems); 
                        strcat(tmp2,"\n");  

                        err = write (events[i].data.fd, tmp2, strlen(tmp2));//write 
                        if (err == -1) { printf("client write failed main thread\n"); }
                    break;
                    case C_ERROR ://error
                        err = write (events[i].data.fd, "Control Error: Invalid Command.\n", 33);//write 
                        if (err == -1) { printf("client write failed main thread\n"); }
                    break;       
                }
                shutdown(events[i].data.fd,SHUT_RDWR);
                close(events[i].data.fd);//close after one control command
            }  
        }           
    } 

    err = pthread_mutex_lock(&sdLock);//lock list
    if(err){printf("SD lock failed\n"); abort();}//catch error
    sdown = 1;
    err = pthread_mutex_unlock(&sdLock);//lock list
    if(err){printf("SD unlock failed\n"); abort();}//catch error

    err = pthread_mutex_lock(&workLock);//lock list
    if(err){printf("Work lock failed\n"); abort();}//catch error

    for(i = 0; i<NTHREADS; i++) {
        err = pthread_cond_broadcast(&waitForWork);//signal all workers
        if(err){printf("Main thread shutdown signal failed\n"); abort();}//catch error
    }

    err = pthread_mutex_unlock(&workLock);//lock list
    if(err){printf("Work unlock failed\n"); abort();}//catch error

    printf("Awaiting thread join\n");
    for(i = 0; i<NTHREADS; i++) {
        err = pthread_join(workerT[i], NULL);//join thread
        if(err){printf("Worker %d thread join failed\n",i); abort();}//catch error
    }
    //close fds and sockets
    shutdown(cfd,SHUT_RDWR);
    close(cfd);
    shutdown(pipefd[0],SHUT_RD);
    close(pipefd[0]);//close read pipe   
    return 0;
}

