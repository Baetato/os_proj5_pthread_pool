/*
 * Copyright(c) 2021-2024 All rights reserved by Heekuck Oh.
 * 이 프로그램은 한양대학교 ERICA 컴퓨터학부 학생을 위한 교육용으로 제작되었다.
 * 한양대학교 ERICA 학생이 아닌 이는 프로그램을 수정하거나 배포할 수 없다.
 * 프로그램을 수정할 경우 2024.06.12, ICT융합학부, 2022088031, 배수연, 스레드풀 구현
 */
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include "pthread_pool.h"

/*
 * 일꾼(작업자) 스레드가 수행할 함수입니다.
 * 큐에서 대기 중인 작업을 하나씩 꺼내서 실행합니다.
 * 큐가 비어 있는 경우 새 작업이 들어올 때까지 기다립니다.
 * 스레드풀이 종료될 때까지 이 과정을 반복합니다.
 */
static void *worker(void *param)
{
    pthread_pool_t *pool = (pthread_pool_t *)param;

    while (true) {
        pthread_mutex_lock(&(pool->mutex));

        // 대기열이 비어 있고, 스레드풀이 실행 중일 때
        while (pool->q_len == 0 && pool->state == ON) {
            pthread_cond_wait(&(pool->empty), &(pool->mutex));  // 작업 대기
        }

        // 스레드풀 종료 조건
        if (pool->state == OFF || (pool->state == STANDBY && pool->q_len == 0)) {
            pthread_mutex_unlock(&(pool->mutex));
            break;
        }

        // 작업 큐에서 작업을 꺼내서 실행
        if (pool->q_len > 0) {
            task_t task = pool->q[pool->q_front];
            pool->q_front = (pool->q_front + 1) % pool->q_size;  // 원형 버퍼 사용
            pool->q_len--;

            pthread_cond_signal(&(pool->full));  // 대기중인 제출 스레드 깨우기
            pthread_mutex_unlock(&(pool->mutex));

            (*(task.function))(task.param);  // 작업 실행
        } else {
            pthread_mutex_unlock(&(pool->mutex));
        }
    }

    pthread_exit(NULL);
    return NULL;
}

/*
 * 스레드풀을 초기화합니다.
 * bee_size는 일꾼 스레드의 개수이고, queue_size는 작업 큐의 크기입니다.
 * bee_size는 POOL_MAXBSIZE를, queue_size는 POOL_MAXQSIZE를 넘을 수 없습니다.
 * 일꾼 스레드와 큐를 위한 메모리를 할당하고 필요한 변수를 초기화합니다.
 * 마지막으로 일꾼 스레드를 생성하여 worker() 함수를 실행하게 합니다.
 * 대기열의 크기가 일꾼 스레드의 수보다 작으면 queue_size를 bee_size로 조정합니다.
 * 초기화에 성공하면 POOL_SUCCESS를, 실패하면 POOL_FAIL을 반환합니다.
 */
int pthread_pool_init(pthread_pool_t *pool, size_t bee_size, size_t queue_size)
{
    if (bee_size > POOL_MAXBSIZE || queue_size > POOL_MAXQSIZE) {
        return POOL_FAIL;  // 입력 크기 검증
    }

    if (queue_size < bee_size) {
        queue_size = bee_size;  // 대기열 크기 조정
    }

    pool->bee_size = bee_size;
    pool->q_size = queue_size;
    pool->q_front = 0;
    pool->q_len = 0;
    pool->state = ON;

    pool->bee = (pthread_t *)malloc(sizeof(pthread_t) * bee_size);  // 스레드 배열 할당
    pool->q = (task_t *)malloc(sizeof(task_t) * queue_size);  // 작업 큐 할당

    if (pool->bee == NULL || pool->q == NULL) {
        if (pool->bee != NULL) free(pool->bee);
        if (pool->q != NULL) free(pool->q);
        return POOL_FAIL;  // 메모리 할당 실패 처리
    }

    if (pthread_mutex_init(&(pool->mutex), NULL) != 0 ||
        pthread_cond_init(&(pool->full), NULL) != 0 ||
        pthread_cond_init(&(pool->empty), NULL) != 0) {
        if (pool->bee != NULL) free(pool->bee);
        if (pool->q != NULL) free(pool->q);
        return POOL_FAIL;  // 동기화 객체 초기화 실패 처리
    }

    for (int i = 0; i < bee_size; i++) {
        if (pthread_create(&(pool->bee[i]), NULL, worker, (void *)pool) != 0) {
            pthread_pool_shutdown(pool, POOL_DISCARD);  // 스레드 생성 실패 시 정리
            return POOL_FAIL;
        }
    }

    return POOL_SUCCESS;
}

/*
 * 스레드풀에 작업을 제출합니다.
 * f는 실행할 함수, p는 함수의 인자, flag는 동작 방식입니다.
 * 대기열이 꽉 찬 경우 flag가 POOL_NOWAIT이면 POOL_FULL을 반환합니다.
 * flag가 POOL_WAIT이면 대기열에 빈 자리가 생길 때까지 기다립니다.
 * 작업 제출에 성공하면 POOL_SUCCESS를 반환합니다.
 */
int pthread_pool_submit(pthread_pool_t *pool, void (*f)(void *p), void *p, int flag)
{
    if (pool == NULL || f == NULL) {
        return POOL_FAIL;  // 입력 검증
    }

    pthread_mutex_lock(&(pool->mutex));

    if (pool->q_len == pool->q_size) {
        if (flag == POOL_NOWAIT) {
            pthread_mutex_unlock(&(pool->mutex));
            return POOL_FULL;  // 대기열이 꽉 찼을 때 POOL_NOWAIT 처리
        } else {
            while (pool->q_len == pool->q_size && pool->state == ON) {
                pthread_cond_wait(&(pool->full), &(pool->mutex));  // 빈 자리가 생길 때까지 대기
            }
        }
    }

    if (pool->state != ON) {
        pthread_mutex_unlock(&(pool->mutex));
        return POOL_FAIL;  // 스레드풀이 실행 중이 아님
    }

    int tail = (pool->q_front + pool->q_len) % pool->q_size;
    pool->q[tail].function = f;
    pool->q[tail].param = p;
    pool->q_len++;

    pthread_cond_signal(&(pool->empty));  // 작업 대기중인 일꾼 스레드 깨우기
    pthread_mutex_unlock(&(pool->mutex));

    return POOL_SUCCESS;
}

/*
 * 스레드풀을 종료합니다.
 * 현재 작업 중인 작업을 마치도록 합니다.
 * how가 POOL_COMPLETE이면 대기열에 남은 모든 작업을 마치고 종료합니다.
 * POOL_DISCARD이면 대기열에 남은 작업을 무시하고 종료합니다.
 * 모든 일꾼 스레드를 종료시키고 할당된 자원을 해제합니다.
 * 종료가 완료되면 POOL_SUCCESS를 반환합니다.
 */
int pthread_pool_shutdown(pthread_pool_t *pool, int how)
{
    if (pool == NULL) {
        return POOL_FAIL;  // 입력 검증
    }

    pthread_mutex_lock(&(pool->mutex));

    if (how == POOL_DISCARD) {
        pool->state = OFF;  // 즉시 종료
    } else if (how == POOL_COMPLETE) {
        pool->state = STANDBY;  // 현재 작업 완료 후 종료
    } else {
        pthread_mutex_unlock(&(pool->mutex));
        return POOL_FAIL;  // 잘못된 인자 처리
    }

    pthread_cond_broadcast(&(pool->empty));  // 모든 일꾼 스레드를 깨우기
    pthread_cond_broadcast(&(pool->full));  // 제출 대기중인 스레드 깨우기
    pthread_mutex_unlock(&(pool->mutex));

    for (int i = 0; i < pool->bee_size; i++) {
        pthread_join(pool->bee[i], NULL);  // 일꾼 스레드 종료 대기
    }

    if (pool->bee != NULL) {
        free(pool->bee);  // 메모리 해제
    }

    if (pool->q != NULL) {
        free(pool->q);  // 메모리 해제
    }

    return POOL_SUCCESS;
}
