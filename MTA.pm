package TipJar::MTA;

use 5.006;
use strict;
use warnings;
use Carp;

require Exporter;

use vars qw/$VERSION $MyDomain $interval $basedir
	 $ReturnAddress $Recipient 
	$AgeBeforeDeferralReport
	$LogToStdout
/;
use Fcntl ':flock'; # import LOCK_* constants
$interval = 17;
$AgeBeforeDeferralReport = 4 * 3600; # four hours

$VERSION = '0.04';

$SIG{CHLD} = 'IGNORE';

chomp($MyDomain = `hostname` || 'poorly configured TipJar::mta server');

my $time;
sub newmessage($);

sub OneWeek(){ 7 * 24 * 3600; };
sub SixHours(){ 6 * 3600; };

sub import{
	shift;	#package name
	$basedir = shift;
	$basedir ||= './MTAdir';
};

$LogToStdout = 0;

sub mylog(@){
	open LOG, ">>$basedir/log/current" or print @_ and return;
	flock LOG, LOCK_EX;
	if($LogToStdout){
		seek STDOUT,2,0;
		print @_;
	}else{
		seek LOG,2,0;
		print LOG @_;
	};
	flock LOG, LOCK_UN;	# flushes before unlocking
};

END {
	mylog "$$ exiting\n";
};

sub run(){

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
		mylog "$$ MTA process number $oldpid is still running\n";
		exit;
	};

	seek PID,0,0;
	print PID "$$\n";
	flock PID, LOCK_UN;
	close PID;
	}

	# immediate dir contains reprioritized deferred objects
	-d "$basedir/immediate" or mkdir "$basedir/immediate",0770;

	# endless top level loop
	mylog "$$ starting fork-and-wait loop\n";
	for(;;){
		fork or last;
		sleep $interval;
	};


	$time=time;
	mylog "$$ ",~~localtime,"\n";

	# process new files if any
	opendir BASEDIR, $basedir;
	my @entries = readdir BASEDIR;
	for my $file (@entries){
		-f "$basedir/$file" or next;
		mylog "$$ processing new message file $file\n";
		# expand and write into temp, then try to
		# deliver each file as it is expanded
		unless(open MESSAGE0, "<$basedir/$file"){
			mylog "$$ Could not open $basedir/$file for reading\n";
			unless(unlink "$basedir/$file"){
				mylog "$$ Could not unlink $basedir/$file\n";
			};
			next;
		};

		flock MESSAGE0, LOCK_EX|LOCK_NB or next;

		# UNLINK ISSUE
		# if your computer cannot read from an unlinked file,
		# uncomment the next line
		# my @MessData = (<MESSAGE0>);
		unless(unlink "$basedir/$file"){
			mylog "$$ Could not unlink $basedir/$file\n";
			next;
		};
		# and comment this next line out
		my @MessData = (<MESSAGE0>);

		my $FirstLine = shift @MessData;
		mylog "$$ from $FirstLine";
		$FirstLine =~ s/\s*<*([^<>\s]*).*$/$1/s;

		my @RecipList;
		my $Recip;

		for(;;){
			my $Recip = shift @MessData;
			$Recip =~ s/\s*<*([^<>\s]+\@[\w\-\.]+).*$/$1/s or last;

			mylog "$$ for $Recip\n";
			push @RecipList, $Recip;
		};


		my $string = 'a';
		foreach $Recip (@RecipList){
			$string++;
			open TEMP, ">$basedir/temp/$time.$$.$string";
			print TEMP "$FirstLine\n$Recip\n",@MessData,"\n";
		};
		close TEMP;


		for my $String ('b'..$string){
			my $M = newmessage "$basedir/temp/$time.$$.$String" or next;
			$M->attempt();	# will skip or requeue or delete
		};

		close MESSAGE0;

	};

	# process all messages in immediate directory
	opendir BASEDIR, "$basedir/immediate" or die "could not open immediate dir: $!";
	@entries = readdir BASEDIR;
	for my $file (@entries){
		my $M = newmessage "$basedir/immediate/$file" or next;
		$M->attempt();	# will skip or requeue or delete
	};

	# reprioritize deferred messages
	my($minieon,$microeon) = $time =~ /^(\d+)(\d\d)\d\d$/;
	opendir QDIR, "$basedir/queue";
	my @directories =
	  grep { /\d/ and $_ <= $minieon and -d "$basedir/queue/$_" }
	    readdir QDIR;

	for my $dir (@directories){	
		opendir QDIR, "$basedir/queue/$dir";
		my @directories2 =
		  grep {
	 /\d/ and ($dir * 100 + $_) < ($minieon*100 + $microeon)
			}
		    readdir QDIR;

		unless (@directories2){
			mylog "$$ removing directory queue/$dir\n";
			rmdir "$basedir/queue/$dir";
			next;
		};

		#move files in these directories into the immediate directory
		for my $dir2 (@directories2){
			opendir QDIR, "$basedir/queue/$dir/$dir2";
			for (   readdir QDIR ){
				-f $_ or next;
				mylog "$$ reprioritizing queue/$dir/$dir2/$_\n";
				rename "$basedir/queue/$dir/$dir2/$_", "$basedir/immediate/$_";
			};
			mylog "$$ removing directory queue/$dir/$dir2\n";
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
	open MESSAGE, "<$messageID" or return undef;
	flock MESSAGE, LOCK_EX|LOCK_NB or return undef;
	chomp ($ReturnAddress = <MESSAGE>);
	chomp ($Recipient = <MESSAGE>);
	bless \$messageID;
};

use Socket;

{ no warnings; sub dnsmx($){
	# look up MXes for domain
	my @mxresults = sort {$a <=> $b} `dnsmx $_[0]`;
	# djbdns program dnsmx provides lines of form /\d+ $domain\n
	return map {/\d+ (\S+)/; $1} @mxresults;
};};

sub getresponse(){
	my ($dash,$response) = ('-','');
	while($dash eq '-'){
		my $line = <SOCK>;
		$response .= $line;
		($dash) = $line =~ /^\d+([\-\ ])/;
	};
	$response;
};


sub attempt{
	# deliver and delete, or requeue; also send bounces if appropriate
	my $message = shift;
	mylog "$$ Attempting $ReturnAddress -> $Recipient\n";
	# Message Data is supposed to start on third line

	my ($Domain) = $Recipient =~ /\@(\S+)/ or goto GoodDelivery;

	my @dnsmxes;
	@dnsmxes = dnsmx($Domain);
	mylog "$$ MX: @dnsmxes\n";
	my $Peerout;

	my $line;

	TryAgain:

	while($Peerout = shift @dnsmxes){

		# connect to $Peerout, smtp
		my @GHBNres = gethostbyname($Peerout) or next;
		my $iaddr = $GHBNres[4]	or next;
        	my $paddr   = sockaddr_in(25, $iaddr);
        	socket(SOCK,
			PF_INET,
			SOCK_STREAM,
			getprotobyname('tcp'))
			or die "$$ socket: $!";

		connect(SOCK, $paddr)  || next ;
		mylog "$$ connected to $Peerout\n";
         	my $oldfh = select(SOCK); $| = 1; select($oldfh);
		goto SMTPsession;

	};

	$line = "Unable to establish SMTP connection to $Domain MX";
	goto ReQueue;

	# talk SMTP
	SMTPsession:	

        # expect 220
        $line = getresponse;
        alarm 60;
        mylog "$$ $line";
	unless($line =~ /^2/){
		mylog "$$ Weird greeting: [$line]\n";
		close SOCK;
		goto TryAgain;
	};
	mylog "$$ $line\n";

        print SOCK "HELO $MyDomain\r\n";
        # expect 250
        $line = getresponse;
        alarm 60;
        mylog "$$ $line";
        unless($line =~ /^250 /){
		mylog "$$ peer not happy with HELO: [$line]\n";
		close SOCK;
		goto TryAgain;
	};

        print SOCK "MAIL FROM: <$ReturnAddress>\r\n";
        # expect 250
        $line = getresponse;
        alarm 60;
        mylog "$$ $line";
        unless($line =~ /^2/){
		mylog "$$ peer not happy with return address: [$line]\n";
		if ($line =~ /^4/){
			goto ReQueue;
		};
		if ($line =~ /^5/){
			goto Bounce;
		};
		mylog "$$ reporting noncompliant SMTP peer [$Peerout]\n";
		goto TryAgain;
	};

        print SOCK "RCPT TO: <$Recipient>\r\n";
        # expect 250
        $line = getresponse;
        alarm 60;
        mylog "$$ $line";
        unless($line =~ /^2/){
		mylog "$$ peer not happy with recipient: [$line]\n";
		if ($line =~ /^4/){
			goto ReQueue;
		};
		if ($line =~ /^5/){
			goto Bounce;
		};
		mylog "$$ reporting noncompliant SMTP peer [$Peerout]\n";
		goto TryAgain;
	};


        print SOCK "DATA\r\n";
        # expect 354
        $line = getresponse;
        alarm 60;
        mylog "$$ $line";
        unless($line =~ /^354 /){
		mylog "$$ peer not happy with DATA: [$line]\n";
		if ($line =~ /^4/){
			goto ReQueue;
		};
		if ($line =~ /^5/){
			goto Bounce;
		};
		mylog "$$ reporting noncompliant SMTP peer [$Peerout]\n";
		goto TryAgain;
	};

	while (<MESSAGE>){
		chomp;
        	alarm 60;
		if ($_ eq '.'){
			print SOCK "..\r\n";
		}else{
			print SOCK $_,"\r\n";
		};
	};
	print SOCK ".\r\n";
        # expect 250
        $line = getresponse;
        alarm 60;
        mylog "$$ $line";
        unless($line =~ /^2 /){
		mylog "$$ peer not happy with message body: [$line]\n";
		if ($line =~ /^4/){
			goto ReQueue;
		};
		if ($line =~ /^5/){
			goto Bounce;
		};
		mylog "$$ reporting noncompliant SMTP peer [$Peerout]\n";
		goto TryAgain;
	};

	mylog "$$ $Peerout: $line\n";
	goto GoodDelivery;

	ReQueue:
	print SOCK "quit\r\n";
	close SOCK;
	$message->requeue($line);
	return undef;

	Bounce:

	$ReturnAddress =~ /\@/ or goto GoodDelivery; #suppress doublebounces
	my $filename = join '.',time,$$,'bounce',rand(10000000);
	open BOUNCE, ">$basedir/temp/$filename";
	print BOUNCE <<EOF;
MAILER-DAEMON
$ReturnAddress
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
	print SOCK "quit\r\n";
	close SOCK;
	return unlink $$message;	# "true"

};

sub requeue{
	my $message = shift;
	my $reason = shift;
	my ($fdir,$fname) = $$message =~ m#^(.+)/([^/]+)$#;
	my @stat = stat($$message);
	my $age = time - $stat[9];

	if ($age > OneWeek){

		$ReturnAddress =~ /\@/ or goto unlinkme; #suppress doublebounces
		my $filename = join '.',time,$$,'bounce',rand(10000000);
		open BOUNCE, ">$basedir/temp/$filename";
		print BOUNCE <<EOF;
MAILER-DAEMON
$ReturnAddress
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
	};

	if (
		$age > $AgeBeforeDeferralReport and
		$reason and
		$ReturnAddress =~ /\@/ # suppress doublebounces
	){
	my $filename = join '.',time,$$,'bounce',rand(10000000);
	open BOUNCE, ">$basedir/temp/$filename";
	print BOUNCE <<EOF;
MAILER-DAEMON
$ReturnAddress
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

		$message->deferralmessage("Will keep attempting until message is over a week old");
	};

	my $futuretime = time + ( $age * ( 3 + rand(2)) / 4);
	my ($dir,$subdir) = $futuretime =~ m/^(\d+)(\d\d)\d\d$/;
	
	-d "$basedir/queue/$dir"
	or mkdir "$basedir/queue/$dir", 0777
	or croak "$$ Permissions problems: $basedir/queue/$dir [$!]\n";

	-d "$basedir/queue/$dir/$subdir"
	or mkdir "$basedir/queue/$dir/$subdir", 0777
	or croak "$$ Permissions problems: $basedir/queue/$dir/$subdir [$!]\n";

	rename $$message, "$basedir/queue/$dir/$subdir/$fname";


};


1;
__END__

=head1 NAME

TipJar::MTA - outgoing SMTP with exponential random backoff.

=head1 SYNOPSIS

  use TipJar::MTA '/var/spool/MTA';	# must be a writable -d
					# defaults to ./MTAdir
  $TipJar::MTA::interval='100';		# the default is 17
  $TipJar::MTA::AgeBeforeDeferralReport=3500; # default is 4 hours
  $TipJar::MTA::MyDomain='cpan.org';	# defaults to `hostname`
					# And awaay we go,
  TipJar::MTA::run();			# logging to STDOUT.
  

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

the <Cdnsmx()> function uses the dnsmx program from the djbdns tool package:
it is abstracted into a function for easy replacement with your preferred
MX lookup tool.

The file system holding the queue must support reading from a file handle
after the file has been unlinked from its directory.  If your computer
can't do this, see the spot in the code near the phrase "UNLINK ISSUE"
and follow the instructions.

For that matter, we also generate some long file names with lots
of dots in them, which could conceivably not be portable.

=head1 HISTORY

=over 8

=item 0.03 17 April 2003

	threw away some inefficient archtecture ideas, such as
	per-domain queues for connection reuse, in order to have
	a working system ASAP.  Testing kill-zero functionality in
	test script.

=item 0.04 20 April 2003

	logging to $basedir/log/current instead of stdout, unless
	$LogToStdout is true.
	$AgeBeforeDeferralReport
	variable to suppress deferral
	bounces when a message has been queued for less than an
	interval.

=back

=head1 To-do list

Patches are welcome.

=over 4

=item connection reuse and per-domain queues

	have deferred messages organized by peer, when the
	deferral is because of connection problems.  Group the
	"immediate" messages by domain so we can reuse a connection
	instead of trying to make a new connection

=item ESMTP

	take advantage of post-RFC-821 features

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
