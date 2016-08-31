<?php
if (!defined('TYPO3_MODE')) {
	die ('Access denied.');
}

	// Include the base class for t3p_scalable:
require_once t3lib_extMgm::extPath($_EXTKEY) . 'class.tx_t3pscalable.php';
	// Define XCLASS accordant to current TYPO3 version:
$TYPO3_CONF_VARS[TYPO3_MODE]['XCLASS']['t3lib/class.t3lib_db.php'] = t3lib_extMgm::extPath($_EXTKEY) . 'class.ux_t3lib_db.php';
?>