<?php
if (!defined('TYPO3_MODE')) {
	die ('Access denied.');
}

	// Include the base class for t3p_scalable:
require_once \TYPO3\CMS\Core\Utility\ExtensionManagementUtility::extPath($_EXTKEY) . 'class.tx_t3pscalable.php';

$GLOBALS['TYPO3_CONF_VARS']['SYS']['Objects']['TYPO3\\CMS\\Core\\Database\\DatabaseConnection'] = array(
    'className' => 'Tx_T3pScalable_Xclass_DatabaseConnection',
);