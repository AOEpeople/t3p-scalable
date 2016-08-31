<?php
/***************************************************************
*  Copyright notice
*
*  (c) 2008-2009 Fernando Arconada fernando.arconada at gmail dot com
*  All rights reserved
*
*
*  The GNU General Public License can be found at
*  http://www.gnu.org/copyleft/gpl.html.
*  A copy is found in the textfile GPL.txt and important notices to the license
*  from the author is found in LICENSE.txt distributed with these scripts.
*
*
*  This script is distributed in the hope that it will be useful,
*  but WITHOUT ANY WARRANTY; without even the implied warranty of
*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*  GNU General Public License for more details.
*
*  This copyright notice MUST APPEAR in all copies of the script!
***************************************************************/


/**
* Extends the functionality of TYPO3_DB - the general database handler.
*
* $Id: class.ux_t3lib_db.php 81619 2013-11-19 10:38:53Z axeljung01 $
*
* @author Fernando Arconada fernando.arconada at gmail dot com
* @version 0.9
 */
class ux_t3lib_DB extends t3lib_DB {
	/**
	 * @var integer
	 */
	const LINK_MASTER = 1;
	/**
	 * @var integer
	 */
	const LINK_SLAVE = 2;

	/**
	 * The default link resource (write/master databases)
	 * @var	resource
	 */
	public $link = FALSE;
	/**
	 * The alternative link resoure (read/slave databases) 
	 * @var	resource
	 */
	public $linkRead = FALSE;
	/**
	 * @var integer
	 */
	private $lastUsedLink = self::LINK_MASTER;

	/**
	 * The controlling t3p_scalable object
	 * @var	tx_t3pscalable
	 */
	protected $t3pscalable;

	/**
	 * Constructs this object.
	 */
	public function __construct() {
		$this->t3pscalable = t3lib_div::makeInstance('tx_t3pscalable');
	}

	/**
	 * Returns information about the character sets supported by the current DBM
	 * This function is important not only for the Install Tool but probably for
	 * DBALs as well since they might need to look up table specific information
	 * in order to construct correct queries. In such cases this information should
	 * probably be cached for quick delivery.
	 *
	 * This is used by the Install Tool to convert tables tables with non-UTF8 charsets
	 * Use in Install Tool only!
	 *
	 * @return	array		Array with Charset as key and an array of "Charset", "Description", "Default collation", "Maxlen" as values
	 */
	function admin_get_charsets() {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::admin_get_charsets();
	}
	/**
	 * Returns information about each field in the $table (quering the DBMS)
	 * In a DBAL this should look up the right handler for the table and return compatible information
	 * This function is important not only for the Install Tool but probably for
	 * DBALs as well since they might need to look up table specific information
	 * in order to construct correct queries. In such cases this information should
	 * probably be cached for quick delivery.
	 *
	 * @param	string		Table name
	 * @return	array		Field information in an associative array with fieldname => field row
	 */
	function admin_get_fields($tableName) {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::admin_get_fields($tableName);
	}
	/**
	 * Returns information about each index key in the $table (quering the DBMS)
	 * In a DBAL this should look up the right handler for the table and return compatible information
	 *
	 * @param	string		Table name
	 * @return	array		Key information in a numeric array
	 */
	function admin_get_keys($tableName) {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::admin_get_keys($tableName);
	}
	/**
	 * Returns the list of tables from the default database, TYPO3_db (quering the DBMS)
	 * In a DBAL this method should 1) look up all tables from the DBMS  of
	 * the _DEFAULT handler and then 2) add all tables *configured* to be managed by other handlers
	 * Usage count/core: 2
	 *
	 * @return	array		Array with tablenames as key and arrays with status information as value
	 */
	function admin_get_tables() {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::admin_get_tables();
	}
	/**
	 * mysql() wrapper function, used by the Install Tool and EM for all queries regarding management of the database!
	 * Usage count/core: 10
	 *
	 * @param	string		Query to execute
	 * @return	pointer		Result pointer
	 */
	function admin_query($query) {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::admin_query($query);
	}

	/**
	 * Creates and executes a DELETE SQL-statement for $table where $where-clause
	 * Usage count/core: 40
	 *
	 * @param	string		Database tablename
	 * @param	string		WHERE clause, eg. "uid=1". NOTICE: You must escape values in this argument with $this->fullQuoteStr() yourself!
	 * @return	pointer		MySQL result pointer / DBAL object
	 */
	function exec_DELETEquery($table, $where) {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::exec_DELETEquery($table, $where);
	}
	/**
	 * Creates and executes an INSERT SQL-statement for $table with multiple rows.
	 *
	 * @param	string		Table name
	 * @param	array		Field names
	 * @param	array		Table rows. Each row should be an array with field values mapping to $fields
	 * @param	string/array		See fullQuoteArray()
	 * @return	pointer		MySQL result pointer / DBAL object
	 */
	public function exec_INSERTmultipleRows($table, array $fields, array $rows, $no_quote_fields = FALSE) {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::exec_INSERTmultipleRows($table, $fields, $rows, $no_quote_fields);
	}
	/**
	 * Creates and executes an INSERT SQL-statement for $table from the array with field/value pairs $fields_values.
	 * Using this function specifically allows us to handle BLOB and CLOB fields depending on DB
	 * Usage count/core: 47
	 *
	 * @param	string		Table name
	 * @param	array		Field values as key=>value pairs. Values will be escaped internally. Typically you would fill an array like "$insertFields" with 'fieldname'=>'value' and pass it to this function as argument.
	 * @param	string/array		See fullQuoteArray()
	 * @return	pointer		MySQL result pointer / DBAL object
	 */
	function exec_INSERTquery($table, $fields_values, $no_quote_fields = FALSE) {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::exec_INSERTquery($table, $fields_values, $no_quote_fields);
	}
	/**
	 * Executes a prepared query.
	 * This method may only be called by t3lib_db_PreparedStatement.
	 *
	 * @param string $query The query to execute
	 * @param array $queryComponents The components of the query to execute
	 * @return pointer MySQL result pointer / DBAL object
	 * @access private
	 */
	public function exec_PREPAREDquery($query, array $queryComponents) {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::exec_PREPAREDquery($query, $queryComponents);
	}
	/**
	 * Creates and executes a SELECT SQL-statement
	 * Using this function specifically allow us to handle the LIMIT feature independently of DB.
	 *
	 * @param	string		List of fields to select from the table. This is what comes right after "SELECT ...". Required value.
	 * @param	string		Table(s) from which to select. This is what comes right after "FROM ...". Required value.
	 * @param	string		Optional additional WHERE clauses put in the end of the query. NOTICE: You must escape values in this argument with $this->fullQuoteStr() yourself! DO NOT PUT IN GROUP BY, ORDER BY or LIMIT!
	 * @param	string		Optional GROUP BY field(s), if none, supply blank string.
	 * @param	string		Optional ORDER BY field(s), if none, supply blank string.
	 * @param	string		Optional LIMIT value ([begin,]max), if none, supply blank string.
	 * @return	pointer		MySQL result pointer / DBAL object
	 */
	public function exec_SELECTquery($select_fields, $from_table, $where_clause, $groupBy='', $orderBy='', $limit='') {
		if (!$this->t3pscalable->isAssuredWriteBackendSession() && !$this->t3pscalable->isAssuredWriteCliDispatch() && !$this->t3pscalable->isAssuredWriteTable($from_table)) {
			$this->lastUsedLink = self::LINK_SLAVE;
			$query = $this->SELECTquery($select_fields, $from_table, $where_clause, $groupBy, $orderBy, $limit);
			$res = mysql_query($query, $this->linkRead);

			if ($this->debugOutput) {
				$this->debug('exec_SELECTquery');
			}
			if ($this->explainOutput) {
				$this->explain($query, $from_table, $this->sql_num_rows($res));
			}
		} else {
			$this->lastUsedLink = self::LINK_MASTER;
			$res = parent::exec_SELECTquery($select_fields, $from_table, $where_clause, $groupBy, $orderBy, $limit);
		}

		return $res;
	}
	/**
	 * Truncates a table.
	 *
	 * @param	string		Database tablename
	 * @return	mixed		Result from handler
	 */
	public function exec_TRUNCATEquery($table) {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::exec_TRUNCATEquery($table);
	}
	/**
	 * Creates and executes an UPDATE SQL-statement for $table where $where-clause (typ. 'uid=...') from the array with field/value pairs $fields_values.
	 * Using this function specifically allow us to handle BLOB and CLOB fields depending on DB
	 * Usage count/core: 50
	 *
	 * @param	string		Database tablename
	 * @param	string		WHERE clause, eg. "uid=1". NOTICE: You must escape values in this argument with $this->fullQuoteStr() yourself!
	 * @param	array		Field values as key=>value pairs. Values will be escaped internally. Typically you would fill an array like "$updateFields" with 'fieldname'=>'value' and pass it to this function as argument.
	 * @param	string/array		See fullQuoteArray()
	 * @return	pointer		MySQL result pointer / DBAL object
	 */
	function exec_UPDATEquery($table, $where, $fields_values, $no_quote_fields = FALSE) {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::exec_UPDATEquery($table, $where, $fields_values, $no_quote_fields);
	}

	/**
	 * Executes query
	 * mysql() wrapper function
	 * Usage count/core: 0
	 *
	 * @param	string		Database name
	 * @param	string		Query to execute
	 * @return	pointer		Result pointer / DBAL object
	 * @deprecated since TYPO3 3.6, will be removed in TYPO3 4.6
	 * @see sql_query()
	 */
	function sql($db, $query) {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::sql($db, $query);
	}
	/**
	 * Returns the error status on the last sql() execution
	 * mysql_error() wrapper function
	 * Usage count/core: 32
	 *
	 * @return	string		MySQL error string.
	 */
	function sql_error() {
		if($this->lastUsedLink === self::LINK_SLAVE) {
			return mysql_error($this->linkRead);
		}
		return mysql_error($this->link);
	}
	/**
	 * Open a (persistent) connection to a MySQL server
	 * mysql_pconnect() wrapper function
	 * Usage count/core: 12
	 *
	 * @param	string		Database host IP/domain
	 * @param	string		Username to connect with.
	 * @param	string		Password to connect with.
	 * @return	pointer		Returns a positive MySQL persistent link identifier on success, or FALSE on error.
	 */
	public function sql_pconnect($TYPO3_db_host, $TYPO3_db_username, $TYPO3_db_password) {
			// mysql_error() is tied to an established connection
			// if the connection fails we need a different method to get the error message
		ini_set('track_errors', 1);
		ini_set('html_errors', 0);

		// you need 2 links to database one for read/write queries (link) and other for read only queries (linkRead)
		if ($GLOBALS['TYPO3_CONF_VARS']['SYS']['no_pconnect']) {
			$this->linkRead = $this->t3pscalable->getDbReadConnection($GLOBALS['t3p_scalable_conf']['db']['readAttempts']);
			$this->link = $this->t3pscalable->getDbWriteConnection($GLOBALS['t3p_scalable_conf']['db']['writeAttempts']);
		} else {
			// You cant balance if it uses persistent connections
			$this->link = @mysql_pconnect($TYPO3_db_host, $TYPO3_db_username, $TYPO3_db_password);
			$this->linkRead = $this->link;
		}
		$error_msg = $php_errormsg;
		ini_restore('track_errors');
		ini_restore('html_errors');

		if (!$this->linkRead) {
				// Using default link  as fallback if read only link is not available:
			t3lib_div::sysLog('Could not connect to read MySQL server: ' . $error_msg, 'Core', 4);
			$this->linkRead = $this->link;
		}
		
		if (!$this->linkRead) {
			t3lib_div::sysLog('Could not connect to any MySQL server: ' . $error_msg, 'Core', 4);
		} else {
			$setDBinit = t3lib_div::trimExplode(chr(10), $GLOBALS['TYPO3_CONF_VARS']['SYS']['setDBinit'],TRUE);
			foreach ($setDBinit as $v) {
				if (mysql_query($v, $this->linkRead) === FALSE) {
					t3lib_div::sysLog('Could not initialize read DB connection with query "' . $v . '": ' . mysql_error($this->linkRead), 'Core', 3);
				}
			}
			foreach ($setDBinit as $v) {
				if (mysql_query($v, $this->link) === FALSE)	{
					t3lib_div::sysLog('Could not initialize write DB connection with query "' . $v . '": ' . mysql_error($this->link), 'Core', 3);
				}
			}
		}

		return $this->link;
	}
	/**
	 * Executes query
	 * mysql_query() wrapper function
	 * Beware: Use of this method should be avoided as it is experimentally supported by DBAL. You should consider
	 *         using exec_SELECTquery() and similar methods instead.
	 * Usage count/core: 1
	 *
	 * @param	string		Query to execute
	 * @return	pointer		Result pointer / DBAL object
	 */
	function sql_query($query) {
		$this->lastUsedLink = self::LINK_MASTER;
		return parent::sql_query($query);
	}
	/**
	 * Select a MySQL database
	 * mysql_select_db() wrapper funct
	 *
	 * @param	string		Database to connect to.
	 * @return	boolean		Returns TRUE on success or FALSE on failure.
	 */
	public function sql_select_db($TYPO3_db) {
			// very important: use the same databasename in all servers
		$ret = @mysql_select_db($TYPO3_db, $this->link);

		if (!$ret) {
			t3lib_div::sysLog('Could not select MySQL database ' . $TYPO3_db . ': ' . mysql_error(), 'Core', 4);
		}
		$ret = @mysql_select_db($TYPO3_db, $this->linkRead);
		if (!$ret) {
			t3lib_div::sysLog('Could not select MySQL database ' . $TYPO3_db . ': ' . mysql_error(), 'Core', 4);
		}

		return $ret;
	}
}


if (defined('TYPO3_MODE') && $TYPO3_CONF_VARS[TYPO3_MODE]['XCLASS']['ext/t3p_scalable/class.ux_t3lib_db.php']) {
	include_once($TYPO3_CONF_VARS[TYPO3_MODE]['XCLASS']['ext/t3p_scalable/class.ux_t3lib_db.php']);
}
?>