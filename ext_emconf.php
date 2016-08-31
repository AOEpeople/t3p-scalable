<?php

########################################################################
# Extension Manager/Repository config file for ext: "t3p_scalable"
#
# Auto generated 12-06-2009 14:51
#
# Manual updates:
# Only the data in the array - anything else is removed by next write.
# "version" and "dependencies" must not be touched!
########################################################################

$EM_CONF[$_EXTKEY] = array(
	'title' => 'TYPO3 more scalable',
	'description' => 't3p_scalable is a powerful designed extension with the idea to get a TYPO3 to be able to adapt its architecture in scenarios of heavy load. So with this extension you will be able to use MySQL replication and Memcached in transparently or semi-transparently way. http://www.typo3performance.com',
	'category' => 'misc',
	'shy' => 0,
	'version' => '1.5.0',
	'dependencies' => '',
	'conflicts' => '',
	'priority' => '',
	'loadOrder' => '',
	'module' => '',
	'state' => 'beta',
	'uploadfolder' => 0,
	'createDirs' => '',
	'modify_tables' => '',
	'clearcacheonload' => 0,
	'lockType' => '',
	'author' => 'Fernando Arconada',
	'author_email' => 'fernando.arconada@gmail.com',
	'author_company' => '',
	'CGLcompliance' => '',
	'CGLcompliance_note' => '',
	'constraints' => array(
		'depends' => array(
			'typo3' => '4.3.0-',
		),
		'conflicts' => array(
		),
		'suggests' => array(
		),
	),
	'_md5_values_when_last_written' => 'a:10:{s:9:"ChangeLog";s:4:"1d3f";s:10:"README.txt";s:4:"ee2d";s:12:"ext_icon.gif";s:4:"6df2";s:17:"ext_localconf.php";s:4:"d8c7";s:14:"doc/manual.sxw";s:4:"e4be";s:19:"doc/wizard_form.dat";s:4:"9ddb";s:20:"doc/wizard_form.html";s:4:"8c90";s:26:"libs/class.t3pscalable.php";s:4:"1417";s:45:"typo3versions/4.2.FF/class.t3lib_userauth.php";s:4:"9b3f";s:42:"typo3versions/4.2.FF/class.ux_t3lib_db.php";s:4:"3840";}',
);

?>