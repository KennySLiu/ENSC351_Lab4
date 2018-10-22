#ifndef KENNY_INCLUDE
#define KENNY_INCLUDE

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>


long double get_time();
long double get_second_time();
long double get_microsecond_time();


long double get_second_time(){
    struct timeval time;
    gettimeofday(&time, NULL);
    return time.tv_sec;
}

long double get_microsecond_time(){
    struct timeval time;
    gettimeofday(&time, NULL);
    return time.tv_usec;
}

long double get_time(){
    long double retval;
    retval = get_second_time();
    retval = retval * 1000000;
    retval += get_microsecond_time();
    return retval;
}





#endif /*KENNY_INCLUDE */
