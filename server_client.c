#include "server.h"

#include <errno.h>

#define ROOM_LIMIT 64
#define NAME_SIZE 30
#define CHAT_PROMPT "\nchat>"

extern pthread_mutex_t mutex;
extern pthread_mutex_t rw_lock;
extern int numReaders;

static pthread_cond_t readers_done = PTHREAD_COND_INITIALIZER;

/* Readers briefly take the gate before registering as active. */
static void begin_read(void)
{
    pthread_mutex_lock(&rw_lock);
    pthread_mutex_lock(&mutex);
    numReaders++;
    pthread_mutex_unlock(&mutex);
    pthread_mutex_unlock(&rw_lock);
}

static void end_read(void)
{
    pthread_mutex_lock(&mutex);
    numReaders--;
    if (numReaders == 0)
        pthread_cond_signal(&readers_done);
    pthread_mutex_unlock(&mutex);
}

/* A writer closes the gate and waits for existing readers to finish. */
static void begin_write(void)
{
    pthread_mutex_lock(&rw_lock);
    pthread_mutex_lock(&mutex);
    while (numReaders > 0)
        pthread_cond_wait(&readers_done, &mutex);
    pthread_mutex_unlock(&mutex);
}

static void end_write(void)
{
    pthread_mutex_unlock(&rw_lock);
}
extern char *server_MOTD;
extern struct node *head;

typedef struct {
    int active;
    /* The session owns this record while it is linked in head. */
    struct node user_record;
    unsigned char rooms[ROOM_LIMIT];
    unsigned char direct[max_clients];
} Session;

static Session sessions[max_clients];
static char room_names[ROOM_LIMIT][NAME_SIZE] = { DEFAULT_ROOM };
static int room_count = 1;

/* Callers must hold read or write access to chat state. */
static int find_user(const char *name)
{
    for (struct node *user = head; user != NULL; user = user->next) {
        if (strcmp(user->username, name) != 0)
            continue;

        for (int i = 0; i < max_clients; i++) {
            if (&sessions[i].user_record == user)
                return i;
        }

        break;
    }
    return -1;
}

static int find_room(const char *name)
{
    for (int i = 0; i < room_count; i++) {
        if (strcmp(room_names[i], name) == 0)
            return i;
    }
    return -1;
}

static int register_user(int client)
{
    for (int i = 0; i < max_clients; i++) {
        Session *user = &sessions[i];
        if (user->active)
            continue;

        memset(user, 0, sizeof(*user));
        user->user_record.socket = client;
        snprintf(user->user_record.username, sizeof(user->user_record.username), "guest%d", client);

        /* A logged-in user may already have chosen this guest name. */
        for (int suffix = 1; find_user(user->user_record.username) >= 0; suffix++) {
            snprintf(user->user_record.username, sizeof(user->user_record.username),
                     "guest%d_%d", client, suffix);
        }

        user->rooms[0] = 1;
        user->user_record.next = head;
        head = &user->user_record;
        user->active = 1;
        return i;
    }
    return -1;
}

static void unregister_user(int id)
{
    Session *user = &sessions[id];
    struct node **link = &head;

    // Unlink before clearing the session that owns the user record.
    while (*link != NULL && *link != &user->user_record)
        link = &(*link)->next;

    if (*link != NULL)
        *link = user->user_record.next;

    for (int i = 0; i < max_clients; i++)
        sessions[i].direct[id] = 0;

    // Clear this user's room memberships and outgoing connections.
    memset(user, 0, sizeof(*user));
}

/* Send the full response and suppress SIGPIPE if the peer has closed. */
static int send_text(int socket, const char *text)
{
    size_t remaining = strlen(text);

    while (remaining > 0) {
        ssize_t sent = send(socket, text, remaining, MSG_NOSIGNAL);
        if (sent < 0 && errno == EINTR)
            continue;
        if (sent <= 0)
            return -1;

        text += sent;
        remaining -= (size_t)sent;
    }

    return 0;
}

static void reply(int socket, const char *message)
{
    char output[MAXBUFF];

    snprintf(output, sizeof(output), "%s" CHAT_PROMPT, message);
    send_text(socket, output);
}

static char *trimwhitespace(char *text)
{
    while (isspace((unsigned char)*text))
        text++;

    char *end = text + strlen(text);
    while (end > text && isspace((unsigned char)end[-1]))
        end--;

    *end = '\0';
    return text;
}

static int valid_name(const char *name)
{
    if (*name == '\0' || strlen(name) >= NAME_SIZE)
        return 0;

    for (; *name != '\0'; name++) {
        unsigned char ch = (unsigned char)*name;
        if (!isprint(ch) || isspace(ch))
            return 0;
    }

    return 1;
}

static void append_name(char *output, size_t capacity, const char *name)
{
    size_t used = strlen(output);
    size_t available = capacity - used;

    /* Keep enough room for the prompt in the final response. */
    if (strlen(name) + sizeof(CHAT_PROMPT) + 1 <= available)
        snprintf(output + used, available, "%s\n", name);
}

static int shares_room(const Session *sender, const Session *recipient)
{
    for (int i = 0; i < room_count; i++) {
        if (sender->rooms[i] && recipient->rooms[i])
            return 1;
    }
    return 0;
}

static void broadcast_message(int id, const char *message)
{
    int recipients[max_clients];
    int count = 0;
    char output[MAXBUFF + NAME_SIZE + 16];

    begin_read();

    const Session *sender = &sessions[id];
    snprintf(output, sizeof(output), "\n::%s> %s" CHAT_PROMPT,
             sender->user_record.username, message);

    for (int i = 0; i < max_clients; i++) {
        if (i == id || !sessions[i].active)
            continue;
        if (!sender->direct[i] && !shares_room(sender, &sessions[i]))
            continue;

        /* Keep a reference to this socket even if its owner disconnects. */
        int socket = dup(sessions[i].user_record.socket);
        if (socket >= 0)
            recipients[count++] = socket;
    }

    end_read();

    for (int i = 0; i < count; i++) {
        send_text(recipients[i], output);
        close(recipients[i]);
    }
}

static void update_room(Session *user, const char *command,
                        const char *name, char output[MAXBUFF])
{
    int room = find_room(name);

    if (strcmp(command, "create") == 0) {
        if (room >= 0)
            snprintf(output, MAXBUFF, "Room already exists.\n");
        else if (room_count == ROOM_LIMIT)
            snprintf(output, MAXBUFF, "Room limit reached.\n");
        else
            strcpy(room_names[room_count++], name);

        return;
    }

    if (room < 0) {
        snprintf(output, MAXBUFF, "Room not found.\n");
        return;
    }

    user->rooms[room] = strcmp(command, "join") == 0;
}

static int handle_line(int id, int client, char *line)
{
    char *message = trimwhitespace(line);
    if (*message == '\0') {
        reply(client, "");
        return 1;
    }

    /* Parse a copy so chat messages retain their original internal spaces. */
    char command_line[MAXBUFF];
    strcpy(command_line, message);

    char *argument = command_line;
    while (*argument != '\0' && !isspace((unsigned char)*argument))
        argument++;

    if (*argument != '\0')
        *argument++ = '\0';
    argument = trimwhitespace(argument);

    const char *command = command_line;

    if (strcmp(command, "exit") == 0 || strcmp(command, "logout") == 0)
        return 0;

    if (strcmp(command, "help") == 0) {
        reply(client,
              "login <username> - login with username\n"
              "create <room> - create a room\n"
              "join <room> - join a room\n"
              "leave <room> - leave a room\n"
              "users - list all users\n"
              "rooms - list all rooms\n"
              "connect <user> - connect to user\n"
              "disconnect <user> - disconnect from user\n"
              "exit - exit chat\n");
        return 1;
    }

    int room_command = strcmp(command, "create") == 0 ||
                       strcmp(command, "join") == 0 ||
                       strcmp(command, "leave") == 0;
    int direct_command = strcmp(command, "connect") == 0 ||
                         strcmp(command, "disconnect") == 0;
    int login_command = strcmp(command, "login") == 0;
    int list_rooms = strcmp(command, "rooms") == 0;
    int list_users = strcmp(command, "users") == 0;

    if (!room_command && !direct_command && !login_command &&
        !list_rooms && !list_users) {
        broadcast_message(id, message);
        reply(client, "");
        return 1;
    }

    if ((room_command || direct_command || login_command) &&
        !valid_name(argument)) {
        reply(client, "Enter one name of 1 to 29 characters.\n");
        return 1;
    }

    char output[MAXBUFF] = "";
    int read_only = list_rooms || list_users;

    if (read_only)
        begin_read();
    else
        begin_write();

    Session *user = &sessions[id];

    if (room_command) {
        update_room(user, command, argument, output);
    } else if (direct_command) {
        int other = find_user(argument);

        if (other < 0) {
            snprintf(output, sizeof(output), "User not found.\n");
        } else if (other == id) {
            snprintf(output, sizeof(output),
                     "Choose a different user.\n");
        } else {
            int connected = strcmp(command, "connect") == 0;
            user->direct[other] = connected;
            sessions[other].direct[id] = connected;
        }
    } else if (login_command) {
        int other = find_user(argument);

        if (other >= 0 && other != id)
            snprintf(output, sizeof(output), "Username already in use.\n");
        else
            strcpy(user->user_record.username, argument);
    } else if (list_rooms) {
        for (int i = 0; i < room_count; i++)
            append_name(output, sizeof(output), room_names[i]);
    } else {
        for (int i = 0; i < max_clients; i++) {
            if (sessions[i].active)
                append_name(output, sizeof(output), sessions[i].user_record.username);
        }
    }

    if (read_only)
        end_read();
    else
        end_write();

    reply(client, output);
    return 1;
}

void *client_receive(void *ptr)
{
    int client = *(int *)ptr;
    free(ptr);

    FILE *input = fdopen(client, "r");
    if (input == NULL) {
        close(client);
        return NULL;
    }

    begin_write();
    int id = register_user(client);
    end_write();

    if (id < 0) {
        send_text(client, "Server is full.\n");
        fclose(input);
        return NULL;
    }

    char line[MAXBUFF];

    if (send_text(client, server_MOTD) == 0) {
        /* Read one command or chat message per line from the TCP stream. */
        while (fgets(line, sizeof(line), input) != NULL) {
            if (strchr(line, '\n') == NULL && !feof(input)) {
                int ch = fgetc(input);

                if (ch != '\n' && ch != EOF) {
                    while ((ch = fgetc(input)) != '\n' && ch != EOF)
                        continue;
                    reply(client, "Message too long.\n");
                    continue;
                }
            }

            if (!handle_line(id, client, line))
                break;
        }
    }

    begin_write();
    unregister_user(id);
    end_write();

    // Shut down the connection even if a broadcast still holds a duplicate fd.
    shutdown(client, SHUT_RDWR);
    fclose(input);
    return NULL;
}