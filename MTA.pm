
package TipJar::MTA;

use strict;
use warnings;
use Carp;

use vars qw/
	$VERSION $MyDomain $interval $basedir
	$ReturnAddress $Recipient 
	$AgeBeforeDeferralReport
	$LogToStdout
	$OnlyOnce
	$LastChild
	$TimeStampFrequency
	$timeout
	$Domain $line
	$ConnectionProblem $dateheader
	$dnsmxpath $ConRetryDelay $ReuseQuota $ReuseQuotaInitial
	@NoBounceRegexList
	$MaxActiveKids
	$FourErrCacheLifetime
	%SMTProutes
/;

$ConRetryDelay = 17 * 60 ;
$FourErrCacheLifetime = 7 * 60;
# $dnsmxpath = 'dnsmx';
$ReuseQuotaInitial = 20;

my $res; # used by Net::DNS

use dateheader;
sub concachetest($);
sub cachepurge();

$TimeStampFrequency = 200; # just under an hour at 17 seconds each

$MaxActiveKids = 5; # just how much spam are we sending?

sub CRLF(){
	"\015\012"
};

use Fcntl ':flock'; # import LOCK_* constants
$interval = 17;
$AgeBeforeDeferralReport = 4 * 3600; # four hours

$VERSION = '0.20';

sub VERSION{
	$_[1] or return $VERSION;
	$_[1] <= 0.14 and croak
	'TipJar::MTA now uses Net::DNS instead of dnsmx';

	$_[1] > $VERSION and croak 
	"you are requesting TipJar::MTA version $_[1] but this is only $VERSION";

	$VERSION;
};


use Sys::Hostname;

$MyDomain = ( hostname() || 'sys.hostname.returned.false' );

my $time;
sub newmessage($);

sub OneWeek(){ 7 * 24 * 3600; };
sub SixHours(){ 6 * 3600; };

sub Scramble($){
	my @a = @{shift(@_)};
	my ($i, $ii);
	my $max = @a;
	for($i = 0; $i < $max; $i++){
		$ii = rand $max;
		@a[$i,$ii] = @a[$ii.$i];
	};
	@a;
};

sub import{
	shift;	#package name
	if (grep {m/^nodns$/i} @_){
		*dnsmx = sub($){
			my $host = lc(shift);
			if (exists $SMTProutes{$host}){
				ref($SMTProutes{$host}) and return Scramble($SMTProutes{$host});
				return $SMTProutes{$host};
			};
			if (exists $SMTProutes{SMARTHOST}){
				ref($SMTProutes{SMARTHOST}) and return Scramble($SMTProutes{$host});
				return $SMTProutes{SMARTHOST};
			};
			die "nodns was specified, byt %SMTProutes has no entry for domain <$host>";

		};	
	}else{
		eval 'use Net::DNS';
		$res = Net::DNS::Resolver->new;
		*dnsmx = \&_dnsmx;
	};
	$basedir = shift;
	$basedir ||= './MTAdir';
	
};

$LogToStdout = 0;

{
my $LogTime = 0;

sub DLsave($);
sub DLpurge();

sub mylog(@);
sub mylog(@){

	if (time - $LogTime > 30){
		$LogTime = time;
		mylog scalar localtime;
	};

	defined $Recipient or $Recipient='no recipient';

	open LOG, ">>$basedir/log/current" or print(@_,"\n") and return;
	flock LOG, LOCK_EX or die "flock: $!";
	if($LogToStdout){
		seek STDOUT,2,0;
		print "$$ $Recipient ",@_;
		print "\n";
	}else{
		seek LOG,2,0;
		print LOG "$$ $Recipient ",@_;
		print LOG "\n";
	};
	flock LOG, LOCK_UN;	# flushes before unlocking
};

};

my $ActiveKids;
$SIG{CHLD} = sub{ $ActiveKids--; wait };

sub run(){

	my $string = 'a' ;
	undef $Recipient ;

	-d $basedir
		or mkdir $basedir,0770
		or die "could not mkdir $basedir: $!" ;

	-w $basedir or croak "base dir <$basedir> must be writable!";

	# log dir contains logs (duh)
	-d "$basedir/log"
		or mkdir "$basedir/log",0770
		or die "could not mkdir $basedir/log: $!" ;

	# queue dir contains deferred messageobjects
	-d "$basedir/queue"
		or mkdir "$basedir/queue",0770
		or die "could not mkdir $basedir/queue: $!" ;

	# domain dir contains lists of queued messages, per domain.
	-d "$basedir/domain"
		or mkdir "$basedir/domain",0770
		or die "could not mkdir $basedir/domain: $!" ;

	# 4error dir contains lists of 4NN-error remote addresses, per domain.
	-d "$basedir/4error"
		or mkdir "$basedir/4error",0770
		or die "could not mkdir $basedir/4error: $!" ;

	# 5error dir contains lists of 5NN-error remote addresses, per domain.
	-d "$basedir/5error"
		or mkdir "$basedir/5error",0770
		or die "could not mkdir $basedir/5error: $!" ;

	# conerror dir contains domains we are having trouble connecting to.
	-d "$basedir/conerror"
		or mkdir "$basedir/conerror",0770
		or die "could not mkdir $basedir/conerror: $!" ;

	# temp dir contains message objects under construction
	-d "$basedir/temp" or mkdir "$basedir/temp",0770
		or die "could not mkdir $basedir/temp: $!" ;


	{	# only one MTA at a time, so we can run this 
		# from cron
	open PID, ">>$basedir/temp/MTApid"; # "touch" sort of
	open PID, "+<$basedir/temp/MTApid"
		or die "could not open pid file '$basedir/temp/MTApid'";
	flock PID, LOCK_EX;
	chomp ( my $oldpid = <PID>);

	if ($oldpid and kill 0, $oldpid){
		print "$$ MTA process number $oldpid is still running\n";
		mylog "MTA process number $oldpid is still running";
		exit;
	};

	seek PID,0,0;
	print PID "$$\n";
	flock PID, LOCK_UN;
	close PID;
	}

	# immediate dir contains reprioritized deferred objects
	-d "$basedir/immediate" or mkdir "$basedir/immediate",0770
		or die "could not mkdir $basedir/immediate: $!" ;

	# endless top level loop
	mylog "starting fork-and-wait loop: will launch every $interval seconds.";
	my $count;
	for(;;){
		++$count % $TimeStampFrequency or
			mylog(time,": ",scalar( localtime)," ",$count);

		rand(100) < 1 and cachepurge; # how long is 17000 seconds?

		if($ActiveKids > $MaxActiveKids){
			mylog "$ActiveKids child procs (more than $MaxActiveKids)";
			sleep (1 + int($interval / 3));
			next;
		};
		# new child drops out of the waiting loop
		$LastChild = fork or last;
		$ActiveKids++;
		if($OnlyOnce){
			mylog "OnlyOnce flag set to [$OnlyOnce]";
			return $OnlyOnce;
		};
		sleep $interval;
	};


	$time=time;

	# process new files if any
	opendir BASEDIR, $basedir;
	my @entries = readdir BASEDIR;
	for my $file (@entries){
		-f "$basedir/$file" or next;
		-s "$basedir/$file" or next;
		mylog "processing new message file $file";
		# expand and write into temp, then try to
		# deliver each file as it is expanded
		unless(open MESSAGE0, "<$basedir/$file"){
			mylog "Could not open $basedir/$file for reading";
			unless(unlink "$basedir/$file"){
				mylog "Could not unlink $basedir/$file";
			};
			next;
		};

		flock MESSAGE0, LOCK_EX|LOCK_NB or next;

		unless(unlink "$basedir/$file"){
			mylog "Could not unlink $basedir/$file";
			next;
		};

		my @MessData = (<MESSAGE0>);
		mylog scalar(@MessData),"lines of message data";

		chomp(my $FirstLine = shift @MessData);
		mylog "from [[$FirstLine]]";
		# never mind $FirstLine =~ s/\s*<*([^<>\s]*).*$/$1/s;

		my @RecipList;
		my $Recip;

		for(;;){
			chomp(my $Recip = shift @MessData);
			unless (@MessData){
				die "no body in message";
			};
			# never mind $Recip =~ s/\s*<*([^<>\s]+\@[\w\-\.]+).*$/$1/s or last;
			$Recip =~ /\@/ or last;
			mylog "for $Recip";
			push @RecipList, $Recip;
			mylog "Recipients: @RecipList";
		};


		foreach $Recip (@RecipList){
			($Domain) = $Recip =~ /\@([\w\-\.]+)/;
			$Domain =~ y/A-Z/a-z/;
			$string++;
			open TEMP, ">$basedir/temp/$time.$$.$string";
			print TEMP "$FirstLine\n$Recip\n",@MessData,"\n";
			close TEMP;
			rename 
			"$basedir/temp/$time.$$.$string",
			"$basedir/immediate/$time.$$.$string";
			mylog 
			DLsave "$basedir/immediate/$time.$$.$string";
		};

	};

	# process all messages in immediate directory
	opendir BASEDIR, "$basedir/immediate"
		or die "could not open immediate dir: $!";
	@entries = readdir BASEDIR;
	for my $file (@entries){
		my $M = newmessage "$basedir/immediate/$file" or next;
		$M->attempt();	# will skip or requeue or delete
		undef $Recipient;
	};

	# reprioritize deferred messages
	my($minieon,$microeon) = $time =~ /^(\d+)(\d\d)\d\d$/;
	opendir QDIR, "$basedir/queue";
	my @directories =
	  grep { /^\d+$/ and $_ <= $minieon and -d "$basedir/queue/$_" }
	    readdir QDIR;

	for my $dir (@directories){	
		opendir QDIR, "$basedir/queue/$dir";
		my @directories2 =
		    grep { /\w/ } (readdir QDIR);

		unless (@directories2){
			mylog "removing directory queue/$dir";
			rmdir "$basedir/queue/$dir";
			next;
		};

		@directories2 = grep {
	 /\d/ and ($dir * 100 + $_) < ($minieon*100 + $microeon)
			} @directories2;

		#move files in these directories into the immediate directory
		for my $dir2 (@directories2){
			opendir QDIR, "$basedir/queue/$dir/$dir2";
			for (   readdir QDIR ){
				-f "$basedir/queue/$dir/$dir2/$_" or next;
				mylog "immanentizing queue/$dir/$dir2/$_";
				rename "$basedir/queue/$dir/$dir2/$_", "$basedir/immediate/$_";
			};
			mylog "removing inner directory queue/$dir/$dir2";
			rmdir "$basedir/queue/$dir/$dir2";
		};
	};	
	exit;
};


# only one active message per process.
# (MESSAGE, $ReturnAddress, $Recipient) are all global.


sub newmessage($){
	#my $pack = shift;
	my $messageID = shift;
	-f $messageID or return undef;
	-s $messageID or do{
		# eliminate freeze on zero-length message files
		unlink $messageID;
		return undef;
	};
	open MESSAGE, "<$messageID" or return undef;
	flock MESSAGE, LOCK_EX|LOCK_NB or return undef;
	chomp ($ReturnAddress = <MESSAGE>);
	chomp ($Recipient = <MESSAGE>);
	bless \$messageID;
};

my $purgecount;
sub purgedir($);
sub purgedir($){
	my $now = time();
	my $dir = shift;
	my $nonempty;
	my @dirs;
	opendir SUBDIR, $dir;
	foreach (readdir SUBDIR){
		/^\.{1,2}$/ and next;
		$nonempty = 1; 
		-d "$dir/$_" and push @dirs, $_;
		-f "$dir/$_" or next;
		my @statresult = stat(_);
		my $mtime = $statresult[9];
		if(($now - $mtime) > (4 * 60 * 60)){
			unlink $_;
			$purgecount++;
		};	
	};
	foreach my $sdir (@dirs) {
		purgedir("$dir/$sdir");
	};
	rmdir $dir unless($nonempty); # patience is a virtue
};


sub cachepurge(){
	$purgecount = 0;
	opendir DIR, "$basedir/4error/"	;
	my @fours = map {"$basedir/4error/$_"} readdir DIR;

	opendir DIR, "$basedir/5error/"	;
	my @fives = map {"$basedir/5error/$_"} readdir DIR;

	foreach ( @fours, @fives ) {
		/error\/\.\.?$/  and next;
		purgedir($_);
	};
	mylog "purged 4XX,5XX cache and eliminated $purgecount entries";

	opendir DIR, "$basedir/conerror/";
	foreach(readdir DIR){ concachetest $_; };

};


sub concache($){
	mylog "caching connection failure to $_[0]";
	open TOUCH, ">>$basedir/conerror/$_[0]";
	print TOUCH '.';
	close TOUCH;
};

sub concachetest($){
	-f "$basedir/conerror/$_[0]" or return undef;
	my @SR = stat(_);
	( time() - $SR[9] ) < $ConRetryDelay and return 1;

	mylog "ready to try connecting to $_[0] again";
	unlink "$basedir/conerror/$_[0]";

	undef;
};

sub cache4($){
	mylog "caching ",$_[0],$line;
	my ($user,$host) = split '@',$_[0],2 or return undef;
	$host =~ y/A-Z/a-z/;
	$host =~ s/([^\w\.\-])/'X'.ord($1).'Y'/ge;
	$user =~ y/A-Z/a-z/;
	$user =~ s/([^\w\.\-])/'X'.ord($1).'Y'/ge;
	-d "$basedir/4error/$host"
		or mkdir "$basedir/4error/$host",0770
		or die "could not mkdir $basedir/4error/$host: $!" ;
	open CACHE, ">$basedir/4error/$host/$user.TMP$$";
	print CACHE time(),"\n$line cached ".localtime()."\n";
	close CACHE;
	rename "$basedir/4error/$host/$user.TMP$$","$basedir/4error/$host/$user";

}
sub cache4test($){
	my ($user,$host) = split '@',$_[0],2 or return undef;
	$host =~ y/A-Z/a-z/;
	$host =~ s/([^\w\.\-])/'X'.ord($1).'Y'/ge;
	$user =~ y/A-Z/a-z/;
	$user =~ s/([^\w\.\-])/'X'.ord($1).'Y'/ge;
	-d "$basedir/4error/$host" or return undef;
	-f "$basedir/4error/$host/$user" or return undef;
	open CACHE, "<$basedir/4error/$host/$user";
	my $ctime;
	($ctime,$line) = <CACHE>;
	close CACHE;
	if ((time() - $ctime ) > $FourErrCacheLifetime ){
		# 4-file is more than seven minutes old
		unlink "$basedir/4error/$host/$user";
		return undef;
	};
	mylog "4cached ", $line;
	return $ctime;
}

sub cache5($){
	mylog "caching ",$_[0],$line;
	my ($user,$host) = split '@',$_[0],2 or return undef;
	$host =~ y/A-Z/a-z/;
	$host =~ s/([^\w\.\-])/'X'.ord($1).'Y'/ge;
	$user =~ y/A-Z/a-z/;
	$user =~ s/([^\w\.\-])/'X'.ord($1).'Y'/ge;
	-d "$basedir/5error/$host"
		or mkdir "$basedir/5error/$host",0770
		or die "could not mkdir $basedir/5error/$host: $!" ;
	open CACHE, ">$basedir/5error/$host/$user.TMP$$" or mylog "CACHEfile: $basedir/5error/$host/$user.TMP$$   $!";
	print CACHE time(),"\n$line cached ".localtime()."\n";
	close CACHE;
	rename "$basedir/5error/$host/$user.TMP$$","$basedir/5error/$host/$user";
}

sub cache5test($){
	my ($user,$host) = split '@',$_[0],2 or return undef;
	$host =~ y/A-Z/a-z/;
	$host =~ s/([^\w\.\-])/'X'.ord($1).'Y'/ge;
	$user =~ y/A-Z/a-z/;
	$user =~ s/([^\w\.\-])/'X'.ord($1).'Y'/ge;
	-d "$basedir/5error/$host" or return undef;
	-f "$basedir/5error/$host/$user" or return undef;
	open CACHE, "<$basedir/5error/$host/$user";
	flock CACHE, LOCK_SH;
	my $ctime;
	($ctime,$line) = <CACHE>;
	close CACHE;
	if ((time() - $ctime ) > ( 4 * 60 * 60 )){
		# 5-file is more than 4 hours old
		unlink "$basedir/5error/$host/$user";
		return undef;
	};
	mylog "5cached ", $line;
	return $ctime;
}


use Socket;

# { no warnings; sub dnsmx($){
# 	# look up MXes for domain
# 	my @mxresults = sort {$a <=> $b} `$dnsmxpath $_[0]`;
# 	# djbdns program dnsmx provides lines of form /\d+ $domain\n
# 	return map {/\d+ (\S+)/; $1} @mxresults;
# };};

# use Net::DNS;  now in Import
# now in import   = Net::DNS::Resolver->new;
sub _dnsmx($){

	my $name = shift;
	my @mx = map {$_->exchange} mx($res,$name);
	@mx or return ($name);

	return  @mx;
};



# my $calls;
# sub SOCKready(){
#         my $rin='';	
#         vec($rin,fileno('SOCK'),1) = 1;	
# 	my ($n, $tl) = select(my $r=$rin,undef,undef,0.25);
# 	print "$calls\n";
# 	$calls++ > 200 and exit;
# 	return $n;
# };

my $CRLF = CRLF;

sub eofSOCK(){
	no warnings;
	my $hersockaddr    = getpeername(SOCK);
	if (defined $hersockaddr){
		return undef;
	}else{
		mylog "SOCK not connected";
		return 1;
	};
};

sub getresponse($){

	# mylog "sending: [$_[0]]";

	if(eofSOCK){
		mylog "problem with SOCK";
		return undef;
	};

	$timeout = 0;
	alarm 130;
	unless(print SOCK  "$_[0]$CRLF"){
		mylog "print SOCK: $!";
		return undef;
	};
	# mylog "sent $_[0]";

	my ($dash,$response) = ('-','');
	while($dash eq '-'){
		my $letter;
		my @letters;
		my $i=0;
		my $more = 1;
		my $BOL = 1;	# "beginning of line"
		do {
			if($timeout){
				mylog "timeout in getresponse";
				return undef;
			};
			if(eofSOCK){
				mylog "eofSOCK";
				return undef;
			};
			sysread(SOCK,$letter,1);
			if ($letter eq "\r" or $letter eq "\n"){
				$more = $BOL;
			}else{
				$BOL = 0;
				if(length($letter)){
					$letters[$i++] = $letter;
					# mylog @letters;
				}else{
					sleep 1;
				};
			};
		} while( $more );

		my $iline = join('',@letters);

	#	mylog "received: [$iline]";
		$response .= $iline;
		($dash) = $iline =~ /^\d+([\-\ ])/;
	};
	$response;
};

my $onioning=0;

sub deferralmessage{
# usage: $message->deferralmessage("reason we are deferring")

	$ReturnAddress =~ /\@/ or return; #suppress doublebounces
	my $filename = join '.',time,'DeferralReport',rand(10000000);
	open BOUNCE, ">$basedir/temp/$filename";
	print BOUNCE <<EOF; # we are moving this into immediate
<>
$ReturnAddress
$dateheader
From: MAILER-DAEMON
To: $ReturnAddress
Subject: delivery deferral to <$Recipient>
Content-type: text/plain

$_[0]

The first eighty lines of the message follow below:
-------------------------------------------------------------
EOF

	seek(MESSAGE,0,0);
	for(1..80){
		defined(my $lin = <MESSAGE>) or last;
		print BOUNCE $lin;
	};
	close BOUNCE;
	rename "$basedir/temp/$filename","$basedir/immediate/$filename";
}
# end sub deferralmessage

sub attempt{
	$onioning or $ReuseQuota = $ReuseQuotaInitial;
	$line='';
	$ConnectionProblem = 0;
	# deliver and delete, or requeue; also send bounces if appropriate
	my $message = shift;
	mylog "Attempting [$ReturnAddress] -> [$Recipient]";
	# Message Data is supposed to start on third line

	########################################
	# reuse sock or define global $Domain
	########################################
	if (defined($Domain) and $Domain and $Recipient =~ /\@$Domain$/i){
		eofSOCK or goto HaveSOCK;
	};

	unless(($Domain) = $Recipient =~ /\@([^\s>]+)/){
		mylog "no domain in recipient [$Recipient], discarding message";
		unlink $$message;
		return;
	};
	$Domain =~ y/A-Z/a-z/;
	########################################
	# $Domain is now defined
	########################################

	if (concachetest $Domain){
		mylog "$Domain connection failure cached";
		goto ReQueue_unconnected;
	};

	my @dnsmxes;
	@dnsmxes = dnsmx($Domain);
	my $dnsmx_count = @dnsmxes;
	mylog "[[$Domain]] MX handled by @dnsmxes";
	unless ( @dnsmxes ){
		mylog "requeueing due to empty dnsmx result";
		goto ReQueue_unconnected;
	};
	my $Peerout;


	cache4test $Recipient and
		goto ReQueue;
	cache5test $Recipient and
		goto Bounce;

	TryAgain:

	while($Peerout = shift @dnsmxes){
		# mylog "attempting $Peerout";

		# connect to $Peerout, smtp
		my @GHBNres;
		unless ( @GHBNres = gethostbyname($Peerout)){
			if ($dnsmx_count == 1 and
			    $Peerout eq $Domain){
				mylog $line="Apparently there is no valid MX for $Domain";
				$ConnectionProblem = 0;
				goto Bounce;
			};
			next;
		};
		my $iaddr = $GHBNres[4]	or next;
        	my $paddr   = sockaddr_in(25, $iaddr);
        	socket(SOCK,
			PF_INET,
			SOCK_STREAM,
			getprotobyname('tcp'))
			or die "$$ socket: $!";

		connect(SOCK, $paddr)  || next ;
		mylog "connected to $Peerout";
         	my $oldfh = select(SOCK); $| = 1; select($oldfh);
		goto SMTPsession;

	};

	concache $Domain;
	mylog "Unable to establish SMTP connection to $Domain MX";
	$ConnectionProblem = 1;
	goto ReQueue_unconnected;


	# talk SMTP
	SMTPsession:	
	$SIG{ALRM} =
	sub { mylog 'TIMEOUT -- caught alarm signal in attempt()'; 
			$message->requeue( "timed out during SMTP interaction" );
			unlink $$message;	# "true"
			$onioning and unlink 
				"$basedir/domain/$Domain.$$";
			exit;
	};


        # expect 220
        alarm 60;
	my $Greetingcounter = 0;
	ExpectGreeting:

	my @GreetArr = ();
	do {
	        eval { defined($line = <SOCK>) or die "no line from socket. [$!]"; };
	        if($@ or ++$Greetingcounter > 20 ){
	           mylog @GreetArr,"Error: $@";
	           close SOCK;
	           goto TryAgain;
	        };
        chomp $line;
        mylog $line;
        push @GreetArr, $line;
       } while (substr($line,0,4) ne '220 ') ; # this condition will enforce greeting compliance
	
	$line = join ' / ',@GreetArr;
	$line =~ s/[\r\n]//g;
	@GreetArr > 1 and  mylog "extended greeting: $line";

        # print SOCK "HELO $MyDomain",CRLF;
        # expect 250
        $line = getresponse "HELO $MyDomain" or goto TryAgain;
	mylog $line;
        unless($line =~ /^250[ \-]/){
		mylog "peer not happy with HELO: [$line]";
		close SOCK;
		goto TryAgain;
	};

	HaveSOCK:
	$line = getresponse "RSET" or goto TryAgain;
       
        # expect 250
        # $line = getresponse;
	# mylog "RSET and got [$line]";
        unless($line =~ /^250[ \-]/){
		mylog "peer not happy with RSET: [$line] will not reuse this connection";
		$ReuseQuota =  0;
		# close SOCK;
		# goto TryAgain;
	};


	# remove angle brackets if any
	$ReturnAddress =~ s/^.*<//;
	$ReturnAddress =~ s/>.*$//;

        $line = getresponse "MAIL FROM: <$ReturnAddress>"  or goto TryAgain;
        mylog "$line";
        unless($line =~ /^[2]/){
		mylog "peer not happy with return address: [$line]";
		if ($line =~ /^[4]/){
			mylog "requeueing";
			goto ReQueue;
		};
		if ($line =~ /^[5]/){
			goto Bounce;
		};
		mylog "and response was neither 2,4 or 5 coded.";
		goto TryAgain;
	};

        # print SOCK "RCPT TO: <$Recipient>\r\n";
        # expect 250
	
	# remove angle brackets if any
	$Recipient =~ s/^.*<//;
	$Recipient =~ s/>.*$//;

        $line = getresponse "RCPT TO: <$Recipient>" or goto TryAgain;
        unless($line =~ /^2/){
		mylog "peer not happy with recipient: [$line]";
		if ($line =~ /^4/){
			cache4 $Recipient;
			mylog "requeueing";
			goto ReQueue;
		};
		if ($line =~ /^5/){
			cache5 $Recipient;
			goto Bounce;
		};
		mylog "reporting noncompliant SMTP peer [$Peerout]";
		goto TryAgain;
	};


        # print SOCK "DATA\r\n";
        # expect 354
        $line = getresponse 'DATA' or goto TryAgain;
        unless($line =~ /^354 /){
		mylog "peer not happy with DATA: [$line]";
		if ($line =~ /^4/){
			mylog "requeueing";
			goto ReQueue;
		};
		if ($line =~ /^5/){
			goto Bounce;
		};
		mylog "reporting noncompliant SMTP peer [$Peerout]";
		goto TryAgain;
	};
	my $linecount;
	my $bytecount;
	while (<MESSAGE>){
		$linecount++;
		$bytecount += length;
		chomp;
		eval{
        	alarm 60;
		if ($_ eq '.'){
			print SOCK "..\r\n" or die $!;
		}else{
			print SOCK $_,"\r\n" or die $!;
		};
		};
		if ($@){
			mylog $@;
			goto TryAgain;
		};
	};
	# print SOCK ".\r\n";
        # expect 250
	mylog "$linecount lines ($bytecount chars) of message data, sending dot";	 # TryAgain will pop the MX list when there are more than 1 MX
        $line = getresponse '.' or goto TryAgain;
        unless($line =~ /^2/){
		mylog "peer not happy with message body: [$line]";
		if ($line =~ /^4/){
			mylog "requeueing";
			goto ReQueue;
		};
		if ($line =~ /^5/){
			goto Bounce;
		};
		mylog "reporting noncompliant SMTP peer [$Peerout]";
		goto TryAgain;
	};

	goto GoodDelivery;

	ReQueue:
	$message->requeue($line);
	goto GoodDelivery;

	ReQueue_unconnected:
	$message->requeue($line);
	return undef;

	Bounce:

	$ReturnAddress =~ /\@/ or goto GoodDelivery; #suppress doublebounces
       # grep {$ReturnAddress =~ m/$_/} @NoBounceRegexList and goto GoodDelivery
       for (@NoBounceRegexList){
               if($ReturnAddress =~ m/$_/){
                       mylog "suppressing bounce to <$ReturnAddress>";
                       goto GoodDelivery;
               };
       };
       mylog "bouncing to <$ReturnAddress>";
	my $filename = join '.',time(),'bounce',rand(10000000);
	open BOUNCE, ">$basedir/temp/$filename";
	defined($line) or $line='unknown reason';
	defined($Recipient) or $Recipient='unknown recipient';
	defined($ReturnAddress) or $ReturnAddress='<>';
	defined($Peerout) or $Peerout='unknown peer';

	print BOUNCE <<EOF;
<>
$ReturnAddress
$dateheader
From: MAILER-DAEMON
To: $ReturnAddress
Subject: delivery failure to <$Recipient>
Content-type: text/plain

While connected to SMTP peer $Peerout,
the $MyDomain e-mail system received the error message

$line

which indicates a permanent error.
The first hundred and fifty lines of the message follow below:
-------------------------------------------------------------
EOF

	seek(MESSAGE,0,0);
	for(1..150){
		defined(my $lin = <MESSAGE>) or last;
		print BOUNCE $lin;
	};
	close BOUNCE;
	rename "$basedir/temp/$filename","$basedir/immediate/$filename";

	GoodDelivery:
	undef $Recipient;
	unlink $$message;	# "true"

	alarm 0;
	if($onioning){
	        mylog "already onioning";
	        return;
	};
	if( -f "$basedir/domain/$Domain"){
		mylog "onioning $Domain";
		open DOMAINLOCK, ">>$basedir/domain/.lock";
		flock DOMAINLOCK, LOCK_EX;
		rename "$basedir/domain/$Domain","$basedir/domain/$Domain.$$";
		flock DOMAINLOCK, LOCK_UN;
		close DOMAINLOCK;
		# sleep 4;	# let any writers finish writing
		local *DOMAINLIST;
		$onioning++;
		open DOMAINLIST, "<$basedir/domain/$Domain.$$";
		while (<DOMAINLIST>){
			chomp;
			-f $_ or next;
			if( --$ReuseQuota < 0 or eofSOCK ){	# no more socket reuse.
				open MOREDOMAIN, ">>$basedir/domain/$Domain";
				flock MOREDOMAIN, LOCK_EX;
				seek MOREDOMAIN,2,0;
					while (<DOMAINLIST>){
						chomp;
						-f $_ or next;
						print MOREDOMAIN "$_\n";
					};
				flock MOREDOMAIN, LOCK_UN;
				close MOREDOMAIN;
				last;
			};
			mylog "reusing sock with $_";
			my $M = newmessage $_; # sets some globals
			$M or next;
			$M->attempt();
			undef $Recipient;
		};
		unlink "$basedir/domain/$Domain.$$";
		$onioning--;
	}else{
	        mylog "no onion file for $Domain";
	};
	

	eofSOCK or mylog getresponse 'QUIT';
	close SOCK;

	return;

};

sub requeue{
	my $message = shift;
	my $reason = shift;
	my ($fdir,$fname) = $$message =~ m#^(.+)/([^/]+)$#;
	my @stat = stat($$message);
	my $age = time() - $stat[9];

	if ($age > OneWeek){

		$ReturnAddress =~ /\@/ or goto unlinkme; #suppress doublebounces
		my $filename = join '.',time,$$,'bounce',rand(10000000);
		open BOUNCE, ">$basedir/temp/$filename";
		print BOUNCE <<EOF;
<>
$ReturnAddress
$dateheader
From: MAILER-DAEMON
To: $ReturnAddress
Subject: delivery failure to <$Recipient>
Content-type: text/plain

A message has been enqueued for delivery for over a week,
the $MyDomain e-mail system is deleting it.

Final temporary deferral reason:
$reason

The first hundred and fifty lines of the message follow below:
-------------------------------------------------------------
EOF

		seek(MESSAGE,0,0);
		for(1..150){
			defined(my $lin = <MESSAGE>) or last;
			print BOUNCE $lin;
		};
		close BOUNCE;
		rename "$basedir/temp/$filename","$basedir/immediate/$filename";

		unlinkme:
		unlink $$message;

		# clean up per-domain queue
		DLpurge;
	};

	if (
		$age > $AgeBeforeDeferralReport and
		$reason and
		$ReturnAddress =~ /\@/ # suppress doublebounces
	){
	my $filename = join '.',time,$$,'bounce',rand(10000000);
	open BOUNCE, ">$basedir/temp/$filename";
	print BOUNCE <<EOF;
<>
$ReturnAddress
$dateheader
From: MAILER-DAEMON
To: $ReturnAddress
Subject: delivery deferral to <$Recipient>
Content-type: text/plain

The $MyDomain e-mail system is not able to deliver
a message to $Recipient right now.
Attempts will continue until the message is over a week old.

Temporary deferral reason:
$reason

The first hundred and fifty lines of the message follow below:
-------------------------------------------------------------
EOF

	seek(MESSAGE,0,0);
	for(1..150){
		defined(my $lin = <MESSAGE>) or last;
		print BOUNCE $lin;
	};
	close BOUNCE;
	rename "$basedir/temp/$filename","$basedir/immediate/$filename";

	}; # if old enough to report as deferred 

	my $futuretime = int(time + 100 + ( $age * ( 3 + rand(2)) / 4));
	# print "futuretime will be $futuretime\n";
	my ($dir,$subdir) = ($futuretime =~ m/^(\d+)(\d\d)\d\d$/);
	# print "dir,subdir is $dir,$subdir\n";
	
	-d "$basedir/queue/$dir"
	or mkdir "$basedir/queue/$dir", 0777
	or croak "$$ Permissions problems: $basedir/queue/$dir [$!]\n";

	-d "$basedir/queue/$dir/$subdir"
	or mkdir "$basedir/queue/$dir/$subdir", 0777
	or croak "$$ Permissions problems: $basedir/queue/$dir/$subdir [$!]\n";


	rename $$message, "$basedir/queue/$dir/$subdir/$fname";
	mylog "message queued to $basedir/queue/$dir/$subdir/$fname";

	$ConnectionProblem and DLsave("$basedir/queue/$dir/$subdir/$fname"); ;
};

sub DLpurge(){
	-f "$basedir/domain/$Domain" or return;
	my @list;
	open DOMAINLISTLOCK, ">>$basedir/domain/.lock"
	   or return mylog "could not open [$basedir/domain/.lock] for append";
	alarm 0;	# we're going to block for the lock
	flock DOMAINLISTLOCK, LOCK_EX;
	open DOMAINLIST, "<$basedir/domain/$Domain";
	chomp(@list = <DOMAINLIST>);
	@list = grep { -f $_ } @list;
	
	if (@list){
	 open DOMAINLIST, ">$basedir/domain/$Domain"
	   or return mylog "could not open [$basedir/domain/$Domain] for clobber";
	 foreach (@list){
		print DOMAINLIST "$_\n";
	 };
	 close DOMAINLIST;
	}else{
	 unlink "$basedir/domain/$Domain";

	};
	 flock DOMAINLISTLOCK, LOCK_UN;
	 close DOMAINLISTLOCK;

};

sub DLsave($){
	open DOMAINLISTLOCK, ">>$basedir/domain/.lock"
	   or return mylog "could not open [$basedir/domain/.lock] for append";
	alarm 0;	# we're going to block for the lock
	flock DOMAINLISTLOCK, LOCK_EX;
	open DOMAINLIST, ">>$basedir/domain/$Domain"
	   or return mylog "could not open [$basedir/domain/$Domain] for append";
	print DOMAINLIST "$_[0]\n";
	close DOMAINLIST;
	flock DOMAINLISTLOCK, LOCK_UN;
	close DOMAINLISTLOCK;

};


1;
__END__

=head1 NAME

TipJar::MTA - outgoing SMTP with exponential random backoff.

=head1 SYNOPSIS

  use TipJar::MTA '/var/spool/MTA';	# must be a writable -d
					# defaults to ./MTAdir
  $TipJar::MTA::interval='100';		# the default is 17
  $TipJar::MTA::TimeStampFrequency='35';	# the default is 200
  $TipJar::MTA::AgeBeforeDeferralReport=7000;	# default is 4 hours
  $TipJar::MTA::MyDomain='peanut.af.mil';	# defaults to `hostname`
                # bouces to certain matching addresses can be suppressed.
  @TipJar::MTA::NoBounceRegexList = map { qr/$_/} (
         '^MDA-bounce-recipient\@tipjar.com$',
         '^RAPNAP\+challenge\+sent\+to\+.+==.+\@pay2send.com$'
    );
					# And away we go,
  TipJar::MTA::run();			# logging to /var/spool/MTA/log/current

alternately,

  use TipJar::MTA '/var/spool/MTA', 'nodns';  # we are sending to
                                              # a restricted set of domains
                                              # or using a smarthost
  %TipJar::MTA::SMTProutes = (
      SMARTHOST => 'smtp_outbound.internal',  # smarthost for forwarding, can be a list too
      'example.com' => # mail to example.com will be randomly routed through these three
          [qw/  smtp1.example.com smtp2.example.com backup-smtp.example.org /],
      'example.net' => 'bad-dog.example.net'  # all mail to example.net goes to bad-dog
  );

  

=head1 DESCRIPTION

On startup, we identify the base directory and make sure we
can write to it, check for and create a few subdirectories,
check if there is an MTA already running and stop if there is,
so that TipJar::MTA can be restarted from cron.

We are not concerned with either listening on port 25 or with
local delivery.  This module implements outgoing SMTP with
exponentially deferred random backoffs on temporary failure.
Future delivery scheduling is determined by what directory
a message appears in.  File age, according to C<stat()>, is
used to determine repeated deferral.

We reuse a socket to a domain if we had trouble connecting to
the MX for that domain in the past, or for multiple new messages
going to the same domain.  We also cache 4XX and 5XX error codes
on recipients for four hours to eliminate a mess of traffic when,
for instance, we have to bounce many messages to the same
bogus return address.  We will get a "550 User Unknown" error
on the first bounce and throw away the others.

Every C<$interval> seconds,  we fork a child process.

A new child process first goes through all new outbound messages
and expands them into individual messages 
and tries to send them.  New messages are to be formatted with the return
address on the first line, then recipient addresses on subsequent lines,
then a blank line (rather, a line with no @ sign), then the body of the message.
The L<TipJar::MTA::queue>
module will help compose such files if needed.

Messages are rewritten into multiple messages when they are for
multiple recipients, and then attempted in the order that the
recipients appeared in the file.

After attempting new messages, a child process attempts all messages
in the "immediate" directory.

After attempting all messages in the immediate directory, a child
process moves deferred messages whose times have arrived into the
immediate directory for processing by later children.

Deferred messages are stored in directories named according
to when a message is to be reattempted. Reattempt times are
assigned at requeueing time to be now plus between three and five
quarters of the message age. Messages more than a week old are
not reattempted.  An undeliverable message that got the maximum
deferrment after getting attempted just shy of the one-week deadline
could conceivably be attempted for the final time fifteen and three
quarters days after it was originally enqueued.  Then it would be
deleted.

An array of regular expressions can be specified, and if any of
them match the sender of a bouncing message, the bouncing is
suppressed, so you don't have to waste time with bounce messages from
bad addresses you're sending challenges to for instance.

The format for new messages is as follows:

=over 4

=item return address

The first line of the message contains the return address.  It
can be bare or contained in angle-brackets.  If there are
angle brackets, the part of the line not in them is discarded.

=item recipient list

All recipients are listed each on their own line.  Recipients
must have at-signs in them.

=item blank line

The first line (after the first line) that does not contain a
@ symbol marks the end of the recipients.  We are not concerned
with local delivery.

=item data

Follow the routing information with the data, starting with
header lines.

=back

=head2 EXPORT

None.


=head1 DEPENDENCIES

the C<dnsmx()> function uses Net::DNS.  Versions 0.14 and previous
use djbdns' dnsmx tool if that's preferable for you -- the old function
is commented out.

The file system holding the queue must support reading from a file handle
after the file has been unlinked from its directory.  If your computer
can't do this, see the spot in the code near the phrase "UNLINK ISSUE"
and follow the instructions.

For that matter, we also generate some long file names with lots
of dots in them, which could conceivably not be portable.

=head1 NODNS OPERATION

beginning with version 0.20, the dependency on Net::DNS can be
skipped by including the term "nodns" on the use line, after the
MTAdir, which must appear, to avoid changing the interface.  When
nodns is declared, all MX lookups will be directly from the
C<%TipJar::MTA::SMTProutes> hash of array references keyed by
lowercased domain names.  If the desired domain does not appear,
the reserved domain name 'SMARTHOST' is looked up as a fallback
or a list of fallbacks.  If no SMARTHOST is declared, an error
will be thrown.

The smtproute is selected from the listed routes randomly.

=head1 HISTORY

=over 8

=item 0.03 17 April 2003

threw away some inefficient archtecture ideas, such as
per-domain queues for connection reuse, in order to have
a working system ASAP.  Testing kill-zero functionality in
test script.

=item 0.04 19 April 2003

logging to $basedir/log/current instead of stdout, unless
$LogToStdout is true.
$AgeBeforeDeferralReport
variable to suppress deferral
bounces when a message has been queued for less than an
interval.


=item 0.05 22 April 2003

slight code and documentation cleanup

=item 0.06 6 May 2003

Testing, testing, testing!  make test on TipJar::MTA::queue before
making test on this module, and you will send me two e-mails.  Now
using Sys::Hostname instead of `hostname` and gracefully handling
absolutely any combination of carriage-returns and line-feeds
as valid line termination.

=item 0.07 1 June 2003

Wrapped all reads and writes to the SMTP socket in C<eval> blocks,
and installed a ALRM signal handler, for better handling of time-out 
conditions.  Also added a $TipJar::MTA::TimeStampFrequency variable
which is how many iterations of the main fork-and-send loop to make
before logging a timestamp.  The default is 200.

=item 0.08 10 June 2003

minor cleanup.

=item 0.09 12 June 2003

AOL and Yahoo.com and who knows how many other sticklers require
angle brackets around return addresses and recipients.  Improved
handling of MXes that we cannot connect to, by defining a
C<ReQueue_unconnected>
entry point in addition to the C<ReQueue> one that we had already..

=item 0.10 20 June 2003

We now bounce mail to domains that ( have no MX records OR there is only one MX record and it is the same as the domain name ) AND we could not resolve the one name. Previously it had been given the full benefit of th doubt.

=item 0.11 late June 2003

implemented domain listing for connection reuse.  New messages and
messages queued due to connection failure (but not 400 codes)
get listed in the per-domain queue.

=item 0.12 18 July 2003

fixed a bug that caused the earlier of multiple messages handled
in the same batch to get clobbered.  Re-engineering domain file locking too.

=item 0.13 20 July 2003

we can now handle multi-line 250 responses.  They exist. Also
fixed a problem with retry deferral. And lowercasing of domain.
And SMTP peers who give you a tiny packet on connection before
they send their 220 greeting. And several "uninitialized value" warnings.

Adding a framework for remembering and caching recipient-based 4* and 5* errors
for four hours.

=item 0.14 23 July 2003

the path to the dnsmx program is now configurable through
a global variable C<$TipJar::MTA::dnsmxpath>

The dependency on the dateheader package is now in the Makefile.PL

we now remember domains we have had trouble connecting to and wait
at least C<$TipJar::MTA::ConRetryDelay> seconds
(defaults to seventeen minutes) after a failed
connection attempt before trying again.

We can handle peers that don't know what to do with a C<RSET>
command.  It no longer shuts us down, rather we remember that
the peer doesn't know how to reset its buffers and we prefer not
to reuse a socket when sending them messages.

=item 0.17 15 Feb 2004

rewrote dnsmx() to use Net::DNS 

added @NoBounceRegexList configuration variable, which is a list
of sender-matching regexes that we don't bounce to -- very useful
when you're running a C/R system and have a lot of bogus addresses
you don't care to hear about

We no longer get confused by multi-line greetings, which may have
been the source of many earlier confusions

log text changes

=item 0.18 30 Mar 2005

new conf variable $MaxActiveKids determines max parallel

fixed problem with zero-length message files

4xx error codes are now only cached $FourErrCacheLifetime seconds (defaults to 7 * 60 )

error code cache cleanup is fixed

=item 0.20 21 May 2007

now support 'nodns' use-line option to suppress loading Net::DNS and SMTProutes hash
to provide hard-coding of mail exchange paths instead.

=back

=head1 To-do list and Known Bugs

Patches are welcome.

=over 4

=item log rolling

there is no rotation of the log in the C<mylog()> function.
C<mylog> does
reopen the file by name on every logging event, though.  Rewriting mylog to
use L<Unix::Syslog> or L<Sys::Syslog> would be cool, but would add dependencies.
Mailing the log to the postmaster every once in a while is easy enough
to do from L<cron>.

=item ESMTP

take advantage of post-RFC-821 features, specifically PIPELINING

=item QMTP

use QMTP when available.

=item local deliveries

add MBOX and MailDir deliveries and some kind of
configuration interface

=back

=head1 AUTHOR

David Nicol, E<lt>davidnico@cpan.orgE<gt>

=head1 SEE ALSO

L<TipJar::MTA::queue>.

=cut
