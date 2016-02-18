#!/usr/bin/perl
use constant {
	TIME_ZONE=>'Europe/Moscow',
	AMOUNT_PARTS=>10,
	TABLE_PARTITION_CONF=>'manage_partitions',
	DEFAULT_PART_FIELD=>'clock',
};
use strict;
use feature 'say';
#use Data::Dumper;
use Time::Piece;
use DBI;
use Sys::Syslog qw(:standard :macros);
use DateTime;
use Getopt::Long::Descriptive;
use POSIX qw(strftime);
use Net::Domain qw (hostname hostfqdn);
our $dbh;
our $tables;
our $db_schema;
my $localHost=lc(hostfqdn());
my ($opt, $usage)=describe_options(
    "$0 %o",
    [ 'host|H=s',	'database host name or IP address', { 'default' => $localHost } ],
    [ 'database|D=s',	'database name (aka table schema name)', { 'default' => 'zabbix' } ],
    [ 'debug|d', 	'turn on verbose logging' ],
    [ 'help|h', 	'print this message and exit' ],
);

print ($usage->text),exit if $opt->help;
my $flDebug=$opt->debug;

openlog('mysql_zbx_part', 'ndelay,pid', LOG_LOCAL0);

require 'aux_common.pm'; 

my $dbenv=getDbCreds() || log_fatal('Cant get DB credentials');
$db_schema=$opt->database;
my $db_host=$opt->host;
if ( !defined($dbenv->{$db_host}) and (my $p=index($db_host,'.'))>0 ) {
	my $s=substr($db_host,0,$p);
	$db_host=$dbenv->{$s}?$s:$db_host;
}

log_fatal('Cant find credentials to connect to database on %s',$db_host) 
	unless defined($dbenv->{$db_host}) and defined($dbenv->{$db_host}{'db_names'}{$db_schema});
my $dbConf=$dbenv->{$db_host}{'db_names'}{$db_schema};

$dbh = DBI->connect(sprintf('DBI:mysql:database=%s;host=%s;mysql_multi_statements=1', $db_schema, $db_host), @{$dbConf}{'login','password'});
log_fatal('Your installation of MySQL does not support table partitioning') unless check_have_partition();

unless (check_table_exists($dbh, TABLE_PARTITION_CONF)) {
	my $sqlInit=do {local $/; <main::DATA>};
	print 'SQL Init: ',$sqlInit,"\n";
	$dbh->do($sqlInit) || die 'Cant create configuration table: '.$dbh->errstr;
	die 'Cant intialize table containing all partitioning configuration' unless check_table_exists($dbh, TABLE_PARTITION_CONF);
}

foreach my $tgtTblName ('history_log','history_text') {
  	if (check_table_has_primkey($dbh, $tgtTblName)) {
    		$dbh->do(<<EOSQL) || die 'Cant prepare target tables for partitioning';
ALTER TABLE `${tgtTblName}` DROP PRIMARY KEY, ADD INDEX `${tgtTblName}_0` (`id`);
ALTER TABLE `${tgtTblName}` DROP KEY `${tgtTblName}_2`;
EOSQL
  	}
}

$tables = $dbh->selectall_hashref('SELECT tablename,period,keep_history,field,compress FROM '.TABLE_PARTITION_CONF, 'tablename');

my $part_tables=$dbh->selectall_hashref(<<EOSQL,[qw(table_name partition_name)]);
SELECT	table_name,
	partition_name,
	LOWER(partition_method) AS partition_method,
	RTRIM(LTRIM(partition_expression)) AS partition_expression,
	partition_description,
	table_rows
FROM
	information_schema.partitions
WHERE
	partition_name IS NOT NULL 
		AND
	table_schema = '$db_schema'
EOSQL

foreach my $tblName (sort keys %{$tables}) {
	unless (defined($part_tables->{$tblName}) || (my $res=init_part_on_table($dbh,$tables,$tblName))->{'result'}) {
		syslog(LOG_ERR, sprintf qq([ERR] Partitioning for "%s" is not found and i cant create partitioning (error: "%s")! The table might be not partitioned. Skipped), 
			$part_tables->{$tblName}),
			$res->{'error'}{'message'};
		next;
	}
	printf "create_next_partition(%s,%s,%s)\n", $tblName, _hsh_ref($part_tables->{$tblName}), $tables->{$tblName}->{'period'} if $flDebug;
	create_next_partition($tblName, $part_tables->{$tblName}, $tables->{$tblName}->{'period'});
	printf "remove_old_partitions(%s,%s,%s,%s)\n", $tblName, _hsh_ref($part_tables->{$tblName}), @{$tables->{$tblName}}{'period','keep_history'} if $flDebug;
	remove_old_partitions($tblName, $part_tables->{$tblName}, @{$tables->{$tblName}}{'period','keep_history'});
}
 
#delete_old_data();
 
$dbh->disconnect(); 

__DATA__
DROP TABLE IF EXISTS `manage_partitions`;
CREATE TABLE `manage_partitions` (
  `tablename` 		VARCHAR(64) NOT NULL			COMMENT 'Table name',
  `period` 		ENUM('day','month','year') NOT NULL 	COMMENT 'Partitioning period - daily, monthly or yearly',
  `keep_history` 	INT(3) UNSIGNED NOT NULL DEFAULT '1' 	COMMENT 'For how many days or months to keep the partitions',
  `field`	 	VARCHAR(64) DEFAULT 'clock' 		COMMENT 'Field which values will be used as a partition criteria',
  `compress`		ENUM('0','1') DEFAULT '0' 		COMMENT 'Compression enabled flag',
  `last_updated` 	DATETIME DEFAULT NULL 			COMMENT 'When a partition was added last time',  
  `comments` 		VARCHAR(128) DEFAULT '1' 		COMMENT 'Comments',
  PRIMARY KEY (`tablename`)
) ENGINE=INNODB;

INSERT INTO manage_partitions 
	(tablename, 		period,  keep_history, 	last_updated, 	compress, 	comments) 
VALUES 
	('history',		'day', 	 186, 		now(), 		'0', 		'Floating-point'),
	('history_uint', 	'day', 	 186, 		now(), 		'0', 		'Integer'),
	('history_str', 	'day', 	 186, 		now(), 		'1', 		'String'),
	('history_text', 	'day', 	 186, 		now(), 		'1', 		'Text'),
	('history_log', 	'day', 	 186, 		now(), 		'1', 		'Logfile-data'),
	('trends', 		'month', 6, 		now(), 		'0',		'Floating-point'),
	('trends_uint',		'month', 6, 		now(), 		'0',		'Integer');