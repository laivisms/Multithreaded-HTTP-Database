The Multithreaded HTTP Database program.

	This program is run by compiling and starting the ResurrectionServer. One may then start
the DataBaseUI, for a shell UI to access the Database.

The Database accepts the following SQL commands: SELECT, UPDATE, DELETE, and INSERT INTO.

The additional command "show tables" returns a list of all existing table names in the database

request commands are not case sensitive, but all names of columns, tables, and information are.

The SELECT command may join multiple tables by writing multiple table names after the FROM, seperated
	by commas. Example: SELECT Title FROM Books, Movies WHERE Books.Title = Movies.Name

This program operates as follows:
	The ResurrectionServer initializes the DBServer and APIServer. The ResurrectionServer then 
	intermittedly pings both the APIServer and DBServer. If any one of them fails to respond,
	it starts them again.
	
	The APIServer then blocks, listening to its port, waiting for an HTTP connection. Once a
	connection is detected, it sends the payload of the HTTP message in a new HTTP message
	to the DBServer's IP. 

	Once the DBServer receives a request, it passes the socket into a new runnable object, puts
	the runnable into a new thread, starts the thread, and continues listening at its port for more
	requests. The main thread does not return the response to the socket: that is the job
	of the newly created thread.

	The new thread then reads the request out of the socket and sends the text of the request to a new
	instance of QueryHandler, which parses the query and determines whether it is a Select, Update,
	Create, Insert, or Show Table, and solves the request accordingly. The answer is then attached
	to a handler that was passed to the QueryHandler by the DBServer Thread. The DBServer Thread
	then reads the response, insterts it into an HTTP message as the payload, and writes the message
	into the socket connected to the client.

