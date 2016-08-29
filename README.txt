=============================
To be added to documentation:
=============================


>>	2010-01-05  Oliver Hader  <oliver@typo3.org>

	Assure Configuration

	This kind of configuration assures that a write/master connection is used
	to handle data. This can be handy if it has to be assured that for e.g.
	session data (or other sensitive data) the accordant database connection
	is used.

	Properties in configuration $t3p_scalable_conf['db']['assure']['write']:

		+ 'tables' (string)
		  Contains a list of tables that shall only be accessed by the
		  write/master connection.
		  Example: 'tables' => 'fe_session_data,fe_sessions,cache_pages'
		+ 'backendSession' (boolean)
		  Defines whether the current backend user sessions shall use the
		  write/master connection.
		  Example: 'backendSession' => true
		+ 'cliDispatch' (boolean)
		  Defines whether the write/master connection shall be used when
		  a CLI process was dispatched. This can be handy e.g. for running
		  a crawler task.

-------------------------------------------------------------------------------

