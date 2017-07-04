# Key/Value Store Server
A simple key/value store server in C using TCP/IP and Telnet

The server uses syncronized POSIX threads to allow clients to access and manipulate the key/value store safely and concurrently.
A PDF report has been included within the repository detailing how the threads have been syncronized.

To Use:
- Run the make command.
- Run the compiled server.c code by specifying the the port number you wish to use for the control port and data port as parameters e.g. './server 8001 8002'.

Connect to control using telnet e.g. 'telnet localhost 8001'.
The control port commands are: 
- 'Count' to return the number of keys in the key value store.
- 'Shutdown' to schedule the server to shutdown when all clients disconnect.

Connect to the data port using telnet e.g. 'telnet 8002'.
The data port commands are:
- 'Put your key your value' replacing 'your key' and 'your value' with the the key and value you wish to add to the store.
- 'Get your key' replacing 'your key' with the key value you wish to return.
- 'Count' to return the number of keys in the key value store.
- 'Delete your key' replacing 'your key' with the key you wish to delete removes the key and its value from the store.
- 'Exists your key' replacing 'your key' with the key that you wish to check exists.
- 'empty line' writing an empty line to the server by pressing enter will disconnect the client from the server.

