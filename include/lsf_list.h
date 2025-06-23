#pragma once

struct lsf_list_head {
    struct lsf_list_head *next;
    struct lsf_list_head* prev;
};

// 初始化链表头
#define LIST_HEAD_INIT(name) { &(name), &(name) }
#define INIT_LIST_HEAD(ptr) do { \
    (ptr)->next = (ptr); \
    (ptr)->prev = (ptr); \
} while (0)

// 添加节点到链表头
static inline void list_add(struct lsf_list_head *new_node, struct lsf_list_head *head) {
    new_node->next = head->next;
    new_node->prev = head;
    head->next->prev = new_node;
    head->next = new_node;
}

// 添加节点到链表尾
static inline void list_add_tail(struct lsf_list_head *new_node, struct lsf_list_head *head) {
    new_node->next = head;
    new_node->prev = head->prev;
    head->prev->next = new_node;
    head->prev = new_node;
}

// 删除节点
static inline void list_del(struct lsf_list_head *entry) {
    entry->prev->next = entry->next;
    entry->next->prev = entry->prev;
    entry->next = entry->prev = NULL;
}

// 遍历宏
#define list_for_each(pos, head) \
    for (pos = (head)->next; pos != (head); pos = pos->next)

// 获取宿主结构体指针
#define container_of(ptr, type, member) \
    (type *)((char *)(ptr) - (char *)&((type *)0)->member)

#define list_entry(ptr, type, member) \
    container_of(ptr, type, member)


