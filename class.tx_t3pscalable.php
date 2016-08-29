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
* Class to have a TYPO3 more scalable
*
* $Id: class.tx_t3pscalable.php 34365 2010-06-15 11:56:50Z ohader $
*
* @author Fernando Arconada fernando.arconada at gmail dot com
* @version 0.9
*/
class tx_t3pscalable {
	protected $assuredWriteTables;
	protected $isAssuredWriteBackendSession;

	protected $assureConfiguration;

	/**
	 * Database servers configurations
	 *
	 * @var array
	 */
	protected $db_config=null;

	/**
	 * Constructs this object.
	 */
	public function __construct() {
		$this->db_config = $GLOBALS['t3p_scalable_conf']['db'];
		$this->assureConfiguration = $GLOBALS['t3p_scalable_conf']['db']['assure'];
		$this->getAssuredWriteTables();
	}

	/**
	 * Private function to get a DB (both, read and write servers)
	 *
	 * @param string $type Type of connection Enum{'read','write'}
	 * @param integer $attempts number of times to try to connect to a db server
	 * @return \mysqli object which represents the connection to a MySQL Server.
	 */
    private function getDbConnection($type, $attempts = 1)
    {
        /* $attempts : number of times to try to connect to a db server
    	    1..n : 1 or more tries choosing servers in a pseudo random fashion
        */
        $db_server = null;
        $link = mysqli_init();
        $connected = false;
		switch($type):
            case 'read':
                $db_server = $this->getReadHost();
                break;
            case 'write':
                $db_server = $this->getWriteHost();
                break;
        endswitch;
        while ($connected === false && $attempts>0) {
            $connected = $link->real_connect(
                $db_server['host'],
                $db_server['user'],
                $db_server['pass'],
                '',
                $db_server['port']
            );
            $attempts--;
        }
        return $link;
    }

	/**
	 * Public wrapper function for getDbConnection only for 'read' servers
	 *
	 * @param int $attempts number of times to try to connect to a db server
	 * @return \mysqli object which represents the connection to a MySQL Server.
	 */
	public function getDbReadConnection($attempts){
		return $this->getDbConnection('read',$attempts);
	}

	/**
	 * Public wrapper function for getDbConnection only for 'write' servers
	 *
	 * @param int $attempts number of times to try to connect to a db server
	 * @return \mysqli object which represents the connection to a MySQL Server.
	 */
	public function getDbWriteConnection($attempts){
		return $this->getDbConnection('write',$attempts);
	}

	/**
	 * Private function to get DB server config array in a random way, the server its selected depending of its weight
	 *
	 * @param string $type Type of connection Enum{'read','write'}
	 * @return array db server config
	 */
	private function getDbHost($type){
		$db_hosts = array();
		foreach ($this->db_config[$type] as $host){
			if(isset($host['weight'])){
				for($i=1;$i<=intval($host['weight']);$i++){
					array_push($db_hosts,$host);
				}
			}else{
				array_push($db_hosts,$host);
			}
		}
		return $db_hosts[rand(0,count($db_hosts)-1)];
	}
	/**
	 * Public wrapper function for getDbHost only for 'read' servers
	 *
	 * @return array db server config
	 */
	public function getReadHost(){
		return $this->getDbHost('read');
	}

	/**
	 * Public wrapper function for getDbHost only for 'write' servers
	 *
	 * @return array db server config
	 */
	public function getWriteHost(){
		return $this->getDbHost('write');
	}

	/**
	 * Gets the tables that are assured to be handled by write/master hosts only.
	 *
	 * @return	array		Tables that are assured to be handled by write/master hosts only.
	 */
	public function getAssuredWriteTables() {
		if (!isset($this->assuredWriteTables)) {
			$this->assuredWriteTables = array();

			if (isset($this->assureConfiguration['write']['tables'])) {
				$this->assuredWriteTables = array_flip(
					t3lib_div::trimExplode(',', $this->assureConfiguration['write']['tables'], true)
				);
			}
		}

		return $this->assuredWriteTables;
	}

	/**
	 * Determines whether a table name is assured to be handled by write/master hosts only.
	 *
	 * @param	string		$table: The table name to be looked up
	 * @return	boolean		Whether a table name is assured to be handled by write/master hosts only
	 */
	public function isAssuredWriteTable($table) {
		$result = false;
		$table = trim($table);

			// Check whether a direct match is successful:
		if (isset($this->assuredWriteTables[$table])) {
			$result = true;
			// Pre-check if it is required to search for tables in the string
			// (could be something like "fe_sessions AS sessions, fe_users"):
		} elseif (strpos($table, ' ') || strpos($table, ',')) {
			if (preg_match_all('/,?\b(\w+)\b(\s+AS\s+\w+)?/i', $table, $matches)) {
				foreach ($matches[1] as $tableItem) {
					if (isset($this->assuredWriteTables[$tableItem])) {
						$result = true;
						break;
					}
				}
			}
		}

		return $result;
	}

	/**
	 * Determines whether a backend user session is assured to be handled by write/master hosts only.
	 *
	 * @return	boolean		Whether a backend user session is assured to be handled by write/master hosts only.
	 */
	public function isAssuredWriteBackendSession() {
		$result = false;
		if (isset($this->assureConfiguration['write']['backendSession']) && $this->assureConfiguration['write']['backendSession']) {
			$result = (isset($GLOBALS['BE_USER']) && isset($GLOBALS['BE_USER']->user['uid']));
		}
		return $result;
	}

	/**
	 * Determines whether a CLI process is dispatched and assured to be handled by write/master hosts only.
	 *
	 * @return	boolean		Whether a CLI process is dispatched and assured to be handled by write/master hosts only.
	 */
	public function isAssuredWriteCliDispatch() {
		$result = false;
		if (isset($this->assureConfiguration['write']['cliDispatch']) && $this->assureConfiguration['write']['cliDispatch']) {
			$result = (defined('TYPO3_cliMode') && TYPO3_cliMode);
		}
		return $result;
	}
}