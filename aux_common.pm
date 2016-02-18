use strict;
our $dbh;
our $tables;
our $db_schema;
use constant {
  USER_DEFINED_CREDS	=>	$ENV{'HOME'}.'/.zbxdb_creds',
  SYSTEM_WIDE_CREDS	=> 	'/etc/zabbix/db_creds',
  ZABBIX_SERVER_CONFIG	=>	'/etc/zabbix/zabbix_server.conf',
};
# Read database credentials into the hash ref ->
# hashRef=getDbCreds([strFilePath])
# strFilePath will be chosen from the set of default pathes if unspecified
# keys(hashRef) = ( 'dflt_host','name','login','password' )
#
# Note:
# 	First host in the first field containing comma-separated host list is the "default" host (usable in db clusters)
sub getDbCreds {
  my $pthDbFile=shift;
  unless ($pthDbFile && -f $pthDbFile && -r $pthDbFile) {
   $pthDbFile=undef;
   foreach ( $ENV{'HOME'}.'/.zbxdb_creds', '/etc/zabbix/db_creds' ) {
    if ( -f $_ && -r $_ ) {
     $pthDbFile=$_;
     last
    }
   }
   return undef unless $pthDbFile;
  }
  my %hrDbCreds;
  open(my $fhCreds,'<',$pthDbFile) || return undef;
  foreach my $hr ( map { chomp; my $h; @{$h}{'host','name','login','password'}=split ':'; $h }(<$fhCreds>) ) {
   my @dbHosts=split ',',$hr->{'host'};
   my %tmplDbConf=(
	'login'=>$hr->{'login'},
	'password'=>$hr->{'password'},
	'dflt_host'=>$dbHosts[0],
   );
   for my $host (@dbHosts) {
    for my $dbName (split ',',$hr->{'name'}) {
      my $dbConf={%tmplDbConf,'name'=>$dbName};
      $hrDbCreds{$host}{'db_names'}{$dbName}=$dbConf;
      push @{$hrDbCreds{$host}{'db_list'}}, $dbConf;
    }
   }
  }  
  close($fhCreds);
  return \%hrDbCreds;
}
# <-

# Or... simply get database credentials from the zabbix server config ->
sub getZbxDbCreds {
 open(my $zbxSrvConf,'<',ZABBIX_SERVER_CONFIG);
 my $hr={ map { chomp; m/^\s*DB(.+?)\s*=\s*([^#]+)\s*/; "\l$1"=>$2 } grep /^\s*DB/,<$zbxSrvConf> };
 close($zbxSrvConf);
 return $hr
}
# <-

sub log_fatal {
	my $msg=sprintf $_[0], @_[1..$#_];
	syslog(LOG_CRIT, $msg);
	die $msg;
}

sub _hsh_ref {
	my $hr=shift || return '';
	unless (ref $hr) {
		$hr=~s%(?<!\\)"%\\"%g;
		return '"'.$hr.'"'
	}
	return '' unless ref $hr eq 'ARRAY' or ref $hr eq 'HASH';
	ref $hr eq 'ARRAY'?
		'['.join(',',map _hsh_ref($_),@{$hr} ).']'
			  :
		'{'.join(',', map {'"'.$_.'" => '._hsh_ref($hr->{$_}) } keys %{$hr}).'}'
}

sub next_date {
  my $period=shift;
  my $t=localtime(defined($_[0])?$_[0]:time);
  my ($y,$m,$d)=($t->year,$t->mon,$t->mday);
  my %p2i=('year'=>0,'month'=>1,'day'=>2);
  if ($period eq 'day') {
    $d=$d==$t->month_last_day?do {$y+=($m==12 && ($m=1)) || ($m++<0); 1 }:$d+1
  } elsif ($period eq 'month') {
    $m=$m==12?do{ $y+=$m==12; 1 }:$m+1;
    $d=1;
  } elsif ($period eq 'year') {
    $y++;
    $m=$d=1;
  }
  return wantarray?($y,,map sprintf('%02g',$_),$m,$d):join('_',map sprintf('%02g',$_), ($y,$m,$d)[0..$p2i{$period}])
}

sub getMySQLver {
	my $v=$_[0]->selectrow_array('SELECT VERSION()');
	$v=~s%^(\d+\.\d+).*$%$1%;
	$v
}

sub get_cur_db_name {
	my $dbh=shift;
	$dbh->selectrow_arrayref('SELECT DATABASE()')->[0]
}

sub check_table_exists {
	my ($dbh,$tableName)=@_;
	$dbh->selectrow_arrayref(sprintf qq{SELECT COUNT(1) FROM information_schema.tables WHERE table_name='%s' AND table_schema='%s'},$tableName, get_cur_db_name($dbh))->[0];
}

sub init_part_on_table {
	my ($dbh,$partConf,$tblName)=@_;
	my $tblPart=$partConf->{$tblName};
	my @dateAfterNow=next_date($tblPart->{'period'});
	my %dp=('day'=>2,'month'=>1);
	my @sqlPars=(
			$tblName,
			$tblPart->{'compress'}?'ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=16':'',
			$tblPart->{'field'} || DEFAULT_PART_FIELD,
			join('_',@dateAfterNow[0..$dp{$tblPart->{'period'}}]),
			join('-',@dateAfterNow)
	);
	my $sqlInitPart=sprintf <<EOSQL,@sqlPars;
ALTER TABLE `%s` %s PARTITION BY RANGE (%s) (
	PARTITION p%s VALUES LESS THAN (UNIX_TIMESTAMP('%s 00:00:00'))
)
EOSQL
	print STDERR '[DBG] INIT PART: '. $sqlInitPart . "\n";
	$dbh->do($sqlInitPart) or return {'result'=>0,'error'=>{'code'=>$dbh->err,'message'=>$dbh->errstr}};
	{'result'=>1}
}

sub check_table_has_primkey {
  my ($dbh,$tblName)=@_;
  grep {$_->[3] eq 'PRI'} @{$dbh->selectall_arrayref("DESC $tblName")}
}

sub check_have_partition {
	my $mysqlVer=getMySQLver($dbh);

	my ($flPartSupported) = $dbh->selectrow_array(
		$mysqlVer<=5.5	?
			qq{SELECT variable_value FROM information_schema.global_variables WHERE variable_name = 'have_partitioning'}
				:
			qq{SELECT plugin_status FROM information_schema.plugins WHERE plugin_name = 'partition'}
	);
 
	$flPartSupported eq scalar($mysqlVer<=5.5?'YES':'ACTIVE');
}
 
sub create_next_partition {
	my ($table_name, $table_part, $period) = @_;
 
	for (my $curr_part = 0; $curr_part < AMOUNT_PARTS; $curr_part++) {
		my $next_name = name_next_part($tables->{$table_name}->{'period'}, $curr_part);
		my $found = 0;
 
		foreach my $partition (sort keys %{$table_part}) {
			if ($next_name eq $partition) {
				syslog(LOG_INFO, "Next partition for $table_name table has already been created. It is $next_name");
				$found = 1;
			}
		}
 
		if ( $found == 0 ) {
			syslog(LOG_INFO, "Creating a partition for $table_name table ($next_name)");
			my $query = 'ALTER TABLE '."$db_schema.$table_name".' ADD PARTITION (PARTITION '.$next_name.
						' VALUES less than (UNIX_TIMESTAMP("'.date_next_part($tables->{$table_name}->{'period'}, $curr_part).'") div 1))';
			syslog(LOG_DEBUG, $query);
			$dbh->do($query);
		}
	}
}
 
sub remove_old_partitions {
	my $table_name = shift;
	my $table_part = shift;
	my $period = shift;
	my $keep_history = shift;
 
	my $curr_date = DateTime->now;
	$curr_date->set_time_zone( TIME_ZONE );
 
	if ( $period eq 'day' ) {
		$curr_date->add(days => -$keep_history);
		$curr_date->add(hours => -$curr_date->strftime('%H'));
		$curr_date->add(minutes => -$curr_date->strftime('%M'));
		$curr_date->add(seconds => -$curr_date->strftime('%S'));
	}
	elsif ( $period eq 'week' ) {
	}
	elsif ( $period eq 'month' ) {
		$curr_date->add(months => -$keep_history);
 
		$curr_date->add(days => -$curr_date->strftime('%d')+1);
		$curr_date->add(hours => -$curr_date->strftime('%H'));
		$curr_date->add(minutes => -$curr_date->strftime('%M'));
		$curr_date->add(seconds => -$curr_date->strftime('%S'));
	}
 
	foreach my $partition (sort keys %{$table_part}) {
		if ($table_part->{$partition}->{'partition_description'} <= $curr_date->epoch) {
			syslog(LOG_INFO, "Removing old $partition partition from $table_name table");
 
			my $query = "ALTER TABLE $db_schema.$table_name DROP PARTITION $partition";
 
			syslog(LOG_DEBUG, $query);
			$dbh->do($query);
		}
	}
}
 
sub name_next_part {
	my $period = shift;
	my $curr_part = shift;
 
	my $name_template;
 
	my $curr_date = DateTime->now;
	$curr_date->set_time_zone( TIME_ZONE );
 
	if ( $period eq 'day' ) {
		my $curr_date = $curr_date->truncate( to => 'day' );
		$curr_date->add(days => 1 + $curr_part);
 
		$name_template = $curr_date->strftime('p%Y_%m_%d');
	}
	elsif ($period eq 'week') {
		my $curr_date = $curr_date->truncate( to => 'week' );
		$curr_date->add(days => 7 * $curr_part);
 
		$name_template = $curr_date->strftime('p%Y_%m_w%W');
	}
	elsif ($period eq 'month') {
		my $curr_date = $curr_date->truncate( to => 'month' );
		$curr_date->add(months => 1 + $curr_part);
 
		$name_template = $curr_date->strftime('p%Y_%m');
	}
 
	return $name_template;
}
 
sub date_next_part {
	my $period = shift;
	my $curr_part = shift;
 
	my $period_date;
 
	my $curr_date = DateTime->now;
	$curr_date->set_time_zone( TIME_ZONE );
 
	if ( $period eq 'day' ) {
		my $curr_date = $curr_date->truncate( to => 'day' );
		$curr_date->add(days => 2 + $curr_part);
		$period_date = $curr_date->strftime('%Y-%m-%d');
	}
	elsif ($period eq 'week') {
		my $curr_date = $curr_date->truncate( to => 'week' );
		$curr_date->add(days => 7 * $curr_part + 1);
		$period_date = $curr_date->strftime('%Y-%m-%d');
	}
	elsif ($period eq 'month') {
		my $curr_date = $curr_date->truncate( to => 'month' );
		$curr_date->add(months => 2 + $curr_part);
 
		$period_date = $curr_date->strftime('%Y-%m-%d');
	}
 
	return $period_date;
}
 
sub delete_old_data {
	$dbh->do('DELETE FROM sessions WHERE lastaccess < UNIX_TIMESTAMP(NOW() - INTERVAL 1 MONTH)');
	$dbh->do('TRUNCATE housekeeper');
	$dbh->do('DELETE FROM auditlog_details WHERE NOT EXISTS (SELECT NULL FROM auditlog WHERE auditlog.auditid = auditlog_details.auditid)');
}

1;
