# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use Test;
BEGIN { plan tests => 7 };
use TipJar::MTA ;
$Tipjar::MTA::LogToStdout = 1;
ok(1); # If we made it this far, we're ok.

#########################

# Insert your test code below, the Test module is use()ed here so read
# its man page ( perldoc Test ) for help writing this test script.

print "testing hostname function\n";
chomp (my $HN = `hostname`);
# print "[$HN,$TipJar::MTA::MyDomain]\n";
ok($HN,$TipJar::MTA::MyDomain);

print "testing dnsmx function\n";
ok(scalar(TipJar::MTA::dnsmx( 'cpan.org' )));

print "Starting, running six seconds, and stopping\n";
$other = fork;
unless($other){ 
	print "$$ using /tmp/MTA_test_dir for basedir in test script\n";
	$TipJar::MTA::basedir = '/tmp/MTA_test_dir';
	TipJar::MTA::run();
};
sleep 3;
print "$$ $other should be running\n";
ok(kill 0,$other);
print "$$ will send $other an alarm signal in another three seconds\n";
$other2 = fork;
unless($other2){ 
	print "$$ using /tmp/MTA_test_dir for basedir in test script\n";
	$TipJar::MTA::basedir = '/tmp/MTA_test_dir';
	TipJar::MTA::run();
}else{
	print "$$: $other2 should exit because of ${other}'s pidfile\n";
	sleep 2;
	print "$$ $other2 should have exited\n";
	ok(! kill 0 => $other2);
};
sleep 2;
print "$$ signalling $other\n";
ok(kill ALRM => $other);
sleep 1;
print "$$ $other should have exited\n";
ok(! kill 0 => $other);




