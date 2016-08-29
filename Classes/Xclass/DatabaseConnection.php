<?php
namespace Aoe\T3p_scalable\Xclass;

use TYPO3\CMS\Core\Utility\GeneralUtility;

/**
* Extends the functionality of TYPO3_DB - the general database handler.
*
* @author Fernando Arconada fernando.arconada at gmail dot com
* @author Juergen Kussmann juergen.kussmann at aoe dot com
*/
class DatabaseConnection extends \TYPO3\CMS\Core\Database\DatabaseConnection
{
    /**
	 * @var integer
	 */
    const LINK_MASTER = 1;
    /**
	 * @var integer
	 */
    const LINK_SLAVE = 2;

    /**
	 * The default link object (write/master databases)
	 * @var	\mysqli
	 */
    protected $link;
    /**
	 * The alternative link object (read/slave databases)
	 * @var	\mysqli
	 */
    protected $linkRead;

    /**
	 * @var integer
	 */
    private $lastUsedLink = self::LINK_MASTER;
    /**
     * The controlling t3p_scalable object
     * @var	tx_t3pscalable
     */
    private $t3pscalable;

    /**
     * Creates and executes a SELECT SQL-statement
     * Using this function specifically allow us to handle the LIMIT feature independently of DB.
     *
     * @param string $select_fields List of fields to select from the table. This is what comes right after "SELECT ...". Required value.
     * @param string $from_table Table(s) from which to select. This is what comes right after "FROM ...". Required value.
     * @param string $where_clause Additional WHERE clauses put in the end of the query. NOTICE: You must escape values in this argument with $this->fullQuoteStr() yourself! DO NOT PUT IN GROUP BY, ORDER BY or LIMIT!
     * @param string $groupBy Optional GROUP BY field(s), if none, supply blank string.
     * @param string $orderBy Optional ORDER BY field(s), if none, supply blank string.
     * @param string $limit Optional LIMIT value ([begin,]max), if none, supply blank string.
     * @return boolean|\mysqli_result|object MySQLi result object / DBAL object
     */
    public function exec_SELECTquery($select_fields, $from_table, $where_clause, $groupBy = '', $orderBy = '', $limit = '') {
        if (
            $this->getT3pScalable()->isAssuredWriteBackendSession() ||
            $this->getT3pScalable()->isAssuredWriteCliDispatch() ||
            $this->getT3pScalable()->isAssuredWriteTable($from_table)
        ) {
            // USE MASTER-DB
            return parent::exec_SELECTquery($select_fields, $from_table, $where_clause, $groupBy, $orderBy, $limit);
        }

        // USE SLAVE-DB
        $query = $this->SELECTquery($select_fields, $from_table, $where_clause, $groupBy, $orderBy, $limit);
        $res = $this->query($query, self::LINK_SLAVE);
        if ($this->debugOutput) {
            $this->debug('exec_SELECTquery');
        }
        if ($this->explainOutput) {
            $this->explain($query, $from_table, $res->num_rows);
        }
        foreach ($this->postProcessHookObjects as $hookObject) {
            /** @var $hookObject PostProcessQueryHookInterface */
            $hookObject->exec_SELECTquery_postProcessAction($select_fields, $from_table, $where_clause, $groupBy, $orderBy, $limit, $this);
        }
        return $res;
    }

    /**
     * Prepares a prepared query.
     *
     * @param string $query The query to execute
     * @param array $queryComponents The components of the query to execute
     * @return \mysqli_stmt|object MySQLi statement / DBAL object
     * @internal This method may only be called by \TYPO3\CMS\Core\Database\PreparedStatement
     */
    public function prepare_PREPAREDquery($query, array $queryComponents)
    {
        $this->lastUsedLink = self::LINK_MASTER;
        return parent::prepare_PREPAREDquery($query, $queryComponents);
    }

    /**
     * Returns the error number on the last query() execution
     *
     * @return integer MySQLi error number
     */
    public function sql_errno()
    {
        if ($this->lastUsedLink === self::LINK_SLAVE) {
            return $this->linkRead->errno;
        }
        return $this->link->errno;
    }

    /**
     * Returns the error status on the last query() execution
     *
     * @return string MySQLi error string.
     */
    public function sql_error()
    {
        if ($this->lastUsedLink === self::LINK_SLAVE) {
            return $this->linkRead->error;
        }
        return $this->link->error;
    }

    /**
     * Open a (persistent) connection to a MySQL server
     *
     * @param string $host Deprecated since 6.1, will be removed in two versions. Database host IP/domain[:port]
     * @param string $username Deprecated since 6.1, will be removed in two versions. Username to connect with.
     * @param string $password Deprecated since 6.1, will be removed in two versions. Password to connect with.
     * @return boolean|void
     * @throws \RuntimeException
     */
    public function sql_pconnect($host = null, $username = null, $password = null)
    {
        if ($this->isConnected) {
            return $this->link;
        }

        if (false === extension_loaded('mysqli')) {
            throw new \RuntimeException('Database Error: PHP mysqli extension not loaded. This is a must have for TYPO3 CMS!', 1271492607);
        }

        if ($host || $username || $password) {
            $this->handleDeprecatedConnectArguments($host, $username, $password);
        }

        if ($this->persistentDatabaseConnection === true) {
            // You cant balance if it uses persistent connections
            $this->link = parent::sql_pconnect($host, $username, $password);
            $this->linkRead = $this->link;
            return $this->link;
        }

        // you need 2 links to database one for read/write queries (link) and other for read only queries (linkRead)
        $this->link = $this->getT3pScalable()->getDbWriteConnection($GLOBALS['t3p_scalable_conf']['db']['writeAttempts']);
        $this->linkRead = $this->getT3pScalable()->getDbReadConnection($GLOBALS['t3p_scalable_conf']['db']['readAttempts']);

        // check, if we have a connection to READ- and WRITE-DB
        if (false === $this->linkRead->ping()) {
            // Using default link as fallback if read only link is not available
            $error_msg = $this->linkRead->connect_error;
            GeneralUtility::sysLog(
                'Could not connect to SLAVE MySQL server: ' . $error_msg,
                'Core',
                GeneralUtility::SYSLOG_SEVERITY_FATAL
            );
            $this->linkRead = $this->link;
        }
        if (false === $this->link->ping()) {
            // @TODO: This should raise an exception. Would be useful especially to work during installation.
            $error_msg = $this->link->connect_error;
            $this->link = null;
            GeneralUtility::sysLog(
                'Could not connect to MySQL server ' . $host . ' with user ' . $username . ': ' . $error_msg,
                'Core',
                GeneralUtility::SYSLOG_SEVERITY_FATAL
            );
            return null;
        }

        $this->isConnected = true;

        $this->initializeDbAfterConnection(self::LINK_MASTER);
        $this->initializeDbAfterConnection(self::LINK_SLAVE);

        // do check charset - if this check is negative, than an exception will be thrown
        $this->checkConnectionCharset();
        return $this->link;
    }

    /**
     * Select a SQL database
     *
     * @param string $TYPO3_db Deprecated since 6.1, will be removed in two versions. Database to connect to.
     * @return boolean Returns TRUE on success or FALSE on failure.
     */
    public function sql_select_db($TYPO3_db = null) {
        if (!$this->isConnected) {
            $this->connectDB();
        }

        if ($TYPO3_db) {
            GeneralUtility::deprecationLog(
                'DatabaseConnection->sql_select_db() should be called without arguments.' .
                ' Use the setDatabaseName() before. Will be removed two versions after 6.1.'
            );
        } else {
            $TYPO3_db = $this->databaseName;
        }

        $isDbSelectedOnMaster = $this->selectDb($TYPO3_db, self::LINK_MASTER);
        $isDbSelectedOnSlave = $this->selectDb($TYPO3_db, self::LINK_SLAVE);
        return ($isDbSelectedOnMaster === true && $isDbSelectedOnSlave === true);
    }

    /**
     * create instance of tx_t3pscalable by lazy-loading
     *
     * Why we do this?
     * Because some unittests backup the variable $GLOBALS (and so, also the variable $GLOBALS['TYPO3_DB']), which means, that this
     * object/class will be serialized/unserialized, so the instance of tx_t3pscalable will be null after unserialization!
     *
     * @return tx_t3pscalable
     */
    protected function getT3pScalable()
    {
        if (false === isset($this->t3pscalable)) {
            $this->t3pscalable = t3lib_div::makeInstance('tx_t3pscalable');
        }
        return $this->t3pscalable;
    }

    /**
     * Disconnect from database if connected
     *
     * @return void
     */
    protected function disconnectIfConnected()
    {
        if ($this->isConnected) {
            $this->link->close();
            $this->linkRead->close();
            $this->isConnected = false;
        }
    }

    /**
     * Central query method. Also checks if there is a database connection.
     * Use this to execute database queries instead of directly calling $this->link->query()
     *
     * @param string $query The query to send to the database
     * @param integer $linkType optional, define, if the query should be executed on read/slave database
     * @return bool|\mysqli_result
     */
    protected function query($query, $linkType = self::LINK_MASTER)
    {
        if (!$this->isConnected) {
            $this->connectDB();
        }

        $this->lastUsedLink = $linkType;
        if ($linkType === self::LINK_MASTER) {
            return $this->link->query($query);
        }
        return $this->linkRead->query($query);
    }

    /**
     * Fixes the SQL mode by unsetting NO_BACKSLASH_ESCAPES if found.
     *
     * @param integer $linkType optional, define, if the query should be executed on read/slave database
     * @return void
     */
    protected function setSqlMode($linkType = self::LINK_MASTER)
    {
        // set SQL-mode on SLAVE-DB
        $resource = $this->query('SELECT @@SESSION.sql_mode;', $linkType);
        if ($resource) {
            $result = $this->sql_fetch_row($resource);
            if (isset($result[0]) && $result[0] && strpos($result[0], 'NO_BACKSLASH_ESCAPES') !== false) {
                $modes = array_diff(GeneralUtility::trimExplode(',', $result[0]), array('NO_BACKSLASH_ESCAPES'));
                $query = 'SET sql_mode=\'' . $this->link->real_escape_string(implode(',', $modes)) . '\';';
                $this->query($query, $linkType);
                GeneralUtility::sysLog(
                    'NO_BACKSLASH_ESCAPES could not be removed from SQL mode: ' . $this->sql_error(),
                    'Core',
                    GeneralUtility::SYSLOG_SEVERITY_ERROR
                );
            }
        }
    }

    /**
     * set charset, set SQL-mode and execute SQL-statements for initialization (if defined)
     *
     * @param integer $linkType
     */
    private function initializeDbAfterConnection($linkType)
    {
        if ($linkType === self::LINK_MASTER) {
            $isCharsetSet = $this->link->set_charset($this->connectionCharset);
        } else {
            $isCharsetSet = $this->linkRead->set_charset($this->connectionCharset);
        }

        if (false === $isCharsetSet) {
            GeneralUtility::sysLog(
                'Error setting connection charset to "' . $this->connectionCharset . '"',
                'Core',
                GeneralUtility::SYSLOG_SEVERITY_ERROR
            );
        }
        foreach ($this->initializeCommandsAfterConnect as $command) {
            if (false === $this->query($command, $linkType)) {
                GeneralUtility::sysLog(
                    'Could not initialize DB connection with query "' . $command . '": ' . $this->sql_error(),
                    'Core',
                    GeneralUtility::SYSLOG_SEVERITY_ERROR
                );
            }
        }
        $this->setSqlMode($linkType);
    }

    /**
     * @param string $dbName
     * @param integer $linkType
     * @return boolean
     */
    private function selectDb($dbName, $linkType)
    {
        $this->lastUsedLink = $linkType;
        if ($linkType === self::LINK_MASTER) {
            $isSelected = $this->link->select_db($dbName);
        } else {
            $isSelected = $this->linkRead->select_db($dbName);
        }

        if ($isSelected === false) {
            GeneralUtility::sysLog(
                'Could not select MySQL database ' . $dbName . ': ' . $this->sql_error(),
                'Core',
                GeneralUtility::SYSLOG_SEVERITY_FATAL
            );
        }

        return $isSelected;
    }
}
