#!/usr/bin/perl -w
#
# backup2archive.pl
# Morgan Jones (morgan@morganjones.org)

# Restore part of an archive from a Zimbra backup.
#
# Based on Adam Cody's instructions: https://wiki.zimbra.com/wiki/Troubleshooting_Course_Content_Rough_Drafts-Recover_Missing_Data_-_User#Redirected_Restore_With_No_Deletes_To_Then_Import_Data_From_A_Specific_Time_Range_Into_A_Subdirectory_Of_Users_Archive_Account
#
# Suggested usage, note that -p refers to the file being written to by
# tee -a.  This allows you to re-start where you left off should the
# script end in an error.
#
# This can't run while the backups are running so you
# should specify an end time (-p) that is 10-15 minutes before you
# backups start.  Ours start at 21:00.
#
# backup2archive.pl -z -p /var/tmp/backup2archive_`zmhostname`.out -e -t 20:45 2>&1 | tee -a /var/tmp/backup2archive_`zmhostname`.out

use Getopt::Std;
use File::Find;
use File::Copy;
use File::Basename;
use strict;
use Data::Dumper;
use IPC::Run qw(start);

sub print_usage();
sub wanted;
sub in_prior_output($);
sub finish_user($$);
sub act_on_signal();
sub check_time($);
sub email_summary_and_exit();

$|=1;

my $z_admin_pass = "pass";
my $domain = "domain.org";

my $recovery_user;
my $recovery = "_recovery_";
my $restored = "_restored_";
my $restore_dir = "Import2015";
my $z_ldap_host = "mldap01." . $domain;
my $mail_from = "morgan\@" . $domain;
my $mail_to = "morgan\@" . $domain . ",kacless\@" . $domain;

my $active_user;

$SIG{'INT'} = \&act_on_signal;
local $ENV{PATH} = "/opt/zimbra/bin:$ENV{PATH}";

my %opts;

getopts('nru:f:p:zc:et:', \%opts);

if (exists $opts{c}) {
    if ($opts{c} !~ /^\d+$/) {
	print "-c must be a number.  Exiting.";
	email_summary_and_exit();
    }
}

print "starting ";
print $opts{c}, " limited "
  if (exists $opts{c});
print " run at ", `date`;


unless (exists $opts{u} || exists $opts{f} || exists $opts{z}) {
    print "you must include one of -u, -f or -z\n";
    print_usage();
}

if (exists $opts{t}) {
    unless ($opts{t} =~ /^\s*\d{1,2}\:\d{2}\s*$/) {
	print "stop time must be in the format h:mm or hh:mm\n\n";
	print_usage();
    }
    print "-t used, script will stop running at (or soon after) $opts{t}\n";
}

print "-c used, limiting processing to $opts{c} accounts\n"
  if (exists $opts{c});

print "-n used, no changes will be made\n"
  if (exists $opts{n});

print "-e used, email will be sent to $mail_to on exit\n"
  if (exists $opts{e});

if (exists $opts{p}) {
    if (defined $opts{p}) {
	print "-p used, file $opts{p} will be used to identify users processed prior\n";
    } else {
	print "-p requires a valid file name\n";
    }
}

if ((exists $opts{u} && exists $opts{f}) || (exists $opts{u} && exists $opts{z}) || 
    (exists $opts{f} && exists $opts{z}) || (exists $opts{f} && exists $opts{z})) {
    print "-u, -f and -z are mutually exclusive, please pick one.\n";
    print_usage();
}

my @users;
my @work_users;
if (exists $opts{u}) {
    @work_users = ($opts{u});
} elsif (exists $opts{f}) {
    if (!defined $opts{f}) {
	print "no file specified, exiting.\n";
	email_summary_and_exit();
    }
    print "-f chosen, opening $opts{f}...\n";
    open (IN, $opts{f}) || die "can't open $opts{f}";
    while (<IN>) {
	chomp;
	push @work_users, $_;
    }
} elsif (exists $opts{z}) {
    print "\ngetting user list...\n";
    @work_users = sort split (/\n/, `zmprov sa zimbramailhost=\`zmhostname\``);

}

for my $u (@work_users) {
    push @users, $u
      unless ($u =~ /^_/ || $u =~ /archive$/);
}

print "up to ", $#users + 1, " accounts to process\n";

my $count=1;
my $total_count=0;
my $prior_found=0;

USERLOOP: for my $user (@users) {
    $total_count++;

    if (exists $opts{t}) {    
	if (check_time($opts{t})) {
	    print "\nit's after $opts{t}, exiting.\n";
	    last;
	}
    }

    my $active_user = $user;

    if (in_prior_output($user)) {
	print "skipping at least one user processed prior\n"
	  if ($prior_found == 0);
	$prior_found = 1;
#	print "$user processed prior, skipping.\n";
	next;
    }

    print "\n\n*** starting on $user ($count";
    print "/" . $opts{c}
      if (exists $opts{c});
    print " total: ${total_count}/", $#users + 1, ") at ", `date`;
    my $archive;


    ## find archive account to which to restore
    print "finding archive account...\n";
    $archive = `zmprov ga $user zimbraarchiveaccount|grep -i zimbraArchiveAccount| awk '{print \$2}'`;
    chomp $archive;

    $archive = $restored . $archive
      if (exists $opts{r});

    if ($archive =~ /^\s*$/) {
	print "$user does not have an archive, skipping\n";
	finish_user($user, 1);
	next;
    }

    print "restoring into archive: $archive\n";

    $recovery_user = $recovery . $user;
    print "\nrestoring $recovery_user...\n";


    ## restore primary account with --skipDeletes to _recovery_ account
#    my $restore_cmd = "zmrestore -d --skipDeletes -a $user -restoreToTime 20150413.123000 -t /opt/zimbra/backup1 -ca -pre " . $recovery;

#    print "$restore_cmd\n";

    my @restore_cmd = qw/zmrestore -d --skipDeletes -a/;
    push @restore_cmd, $user;
    push @restore_cmd, qw/-restoreToTime 20150413.123000 -t \/opt\/zimbra\/backup1 -ca -pre/;
    push @restore_cmd, $recovery;

    print join (' ', @restore_cmd);

    unless (exists $opts{n}) {
	my $restore_h = start \@restore_cmd, '2>pipe', \*ERR;
	my $restore_rc = finish $restore_h;


	my @restore_err;
	while (<ERR>) {
	    push @restore_err, $_;
	}
	close ERR;

	my $restore_err = join ' ', @restore_err;
    
	print "$restore_err";
	if ($restore_err =~ /not found in backup/) {
	    print "$user is not in the backup, skipping.\n";

	    finish_user($user, 1);
	    next;
	}

	if ($restore_err =~ /Missing full backup earlier than restore-to time for account/) {
	    print "$user is missing a full backup, skipping.\n";

	    finish_user($user, 1);
	    next;
	}

	if (!$restore_rc) {
	    print $restore_err;
    	    print "\nrestore failed, trying with --ignoreRedoErrors\n";
    	    my $restore_ignoreredo_cmd = "zmrestore -d --skipDeletes --ignoreRedoErrors -a $user -restoreToTime 20150413.123000 -t /opt/zimbra/backup1 -ca -pre " . $recovery;
    	    print $restore_ignoreredo_cmd . "\n";
    	    if (system ($restore_ignoreredo_cmd)) {
    		print "\nrestore failed with --ignoreRedoErrors, giving up\n";
    		cleanup();
    		email_summary_and_exit();
    	    }
    	}


    }


    ## remove amavisArchiveQuarantineTo from _recovery_ account to save archive licenses
    print "removing archive from $recovery_user...\n";
    my $cmd = "zmprov ma $recovery_user amavisArchiveQuarantineTo ''";
    print $cmd . "\n";
    unless (exists $opts{n}) {
    	system ($cmd);
    }


    ## remove dist list memberships
    my $groups = `ldapsearch -LLL -x -w $z_admin_pass -D cn=config -H ldap://$z_ldap_host zimbraMailForwardingAddress=$recovery_user dn mail|grep mail:|awk '{print \$2}'`;
    my @groups = split (/\n/, $groups);
    print "\nremoving dist list memberships: " . join (' ', @groups) . "\n";

    unless (exists $opts{n}) {
    	open (ZMPROV, "|zmprov");
    	for my $g (@groups) {
    	    print ZMPROV "mdl $g -zimbraMailForwardingAddress $recovery_user\n";
    	}
    	close (ZMPROV);
    	print "\n";
    }

    print "\n";

    while (1) {
    ## export mail from 4/10 to 4/13
	print "exporting mail from 4/10/15 to 4/13/15...\n";

	my @cmd = qw/zmmailbox -z -t 0 -m/;
	push @cmd, $recovery_user;
	push @cmd, "gru";
	push @cmd, "'//?fmt=tgz&query=under:/ after:4/9/15 AND before:4/14/15'";
    
	print join (' ', @cmd, "\n");

	unless (exists $opts{n}) {
	    ##   use IPC::Run instead of system as stderr must be captured to tell the
	    ##   difference between a failure because no mail was exported for
	    ##   that time range and a failure for another reason

	    my $h = start \@cmd, '>', '/var/tmp/msgs.tgz', '2>pipe', \*ERR;
	    my $rc = finish $h;

	    my @err;
	    while (<ERR>) {
		push @err, $_;
	    }
	    close ERR;

	    my $err = join ' ', @err;

	    print "$err";
	    if ($err =~ /status=204.  No data found/) {
		print "$user: no data to import, skipping.\n";
		cleanup();
# here?
		$count++;
		if (exists ($opts{c})) {
		    if ($count > $opts{c}) {
			print "\nStopped processing at requested count $opts{c}, exiting.\n";
			last USERLOOP;
		    }
		}
		print "finished $user at ", `date`;
		next USERLOOP;
	    } elsif ($err =~ /Internal Server Error/) {
		print "Internal Server Error, waiting 10 minutes and retrying...\n";
		sleep 600;
		next;
	    }


	    if (!$rc) {
		print $err;
		print "export failed, exiting.\n";
		cleanup();
		email_summary_and_exit();
	    }
	}
	last;
    }

    ## decompress exported files, move to $restore_dir and compress for input via pru
    my $decompress_cmd = "(mkdir /var/tmp/$restore_dir && mkdir /var/tmp/msgs && cd /var/tmp/msgs && tar xfz ../msgs.tgz)";
    print $decompress_cmd . "\n";
    unless (exists $opts{n}) {
    	if (system ($decompress_cmd)) {
    	    print "decompress messages failed, exiting.\n";
    	    cleanup();
    	    email_summary_and_exit();
    	}
    }

    print "\n";
    print "moving messages to $restore_dir...\n";
    unless (exists $opts{n}) {
    	find (\&wanted, qw:/var/tmp/msgs:);
    }

    my $compress_cmd = "(cd /var/tmp && tar cfz $restore_dir.tgz $restore_dir)";
    print "$compress_cmd\n";
    unless (exists $opts{n}) {
    	if (system ($compress_cmd)) {
    	    print "compress failed, exiting.\n";
    	    cleanup();
    	    email_summary_and_exit();
    	}
    }


    print "\n";
    print "importing messages to $archive\n";
    my $import_cmd = "zmmailbox -z -m $archive pru \"//?fmt=tgz&subfolder=$restore_dir\" /var/tmp/$restore_dir.tgz";
    print $import_cmd . "\n";
    unless (exists $opts{n}) {
    	if (system ($import_cmd)) {
    	    print "import failed, exiting.\n";
    	    cleanup();
    	    email_summary_and_exit();
    	}
    }

    cleanup();

    $count++;
    if (exists $opts{c}) {
	if ($count > $opts{c}) {
	    print "\nStopped processing at requested count $opts{c}, exiting.\n";
	    last;
	}
    }

    if (exists $opts{n}) {
	# print a slightly different message for a dry run so the
	# script doesn't skip this user on future runs
	print "finished (dry) $user at ", `date`;
    } else {
	print "finished $user at ", `date`;
    }
}

print "finished run at ", `date`;
email_summary_and_exit();

sub print_usage() {
    print "\n";
    print "usage: $0 [-n] [-r] [-c <count>]\n";
    print "\t[-p <previous output file>] [-e]\n";
    print "\t-f <time> -u <user> | -f <user list file> | -z \n";
    print "-n print only, do not make changes\n";
    print "-r restore to an archive prefaced with contents of \$restored\n";
    print "-c <count> stop after count users\n";
    print "-p <previous output file> use previous output file from this script to determine\n";
    print "\tusers that have already been processed\n";
    print "\t-e send an email summary\n";
    print "\t-f <time> choose a stop time in hh:mm, 0-23, 00-60.\n";
    print "\tMore than 24 run time not supported\n";
    print "-u <user> | -f <user list file> | -z mutually exclusive: work on one user (-u),\n";
    print "\ta list of users (-f) or get a list from zmprov sa zimbramailhost=\`zmhostname\`\n";
    print "\n";
    email_summary_and_exit();
}


sub wanted {
    if ($File::Find::name =~ /\.eml$/) {
	if (!move ($File::Find::name, "/var/tmp/$restore_dir")) {
	    print "moving $File::Find::name failed, exiting\n";
	    cleanup();
	    email_summary_and_exit();
	}
    }
}

sub cleanup {
    print "\n";
    print "cleaning up...\n";
    if (defined $recovery_user) {
	print "removing $recovery_user...\n";
	unless (exists $opts{n}) {
	    my $remove_recovery_cmd = "zmprov da $recovery_user";
	    system ($remove_recovery_cmd);
	}
    }

    print "removing temp directories...\n";
    unless (exists $opts{n}) {
	my $rm_cmd = "rm -rf /var/tmp/msgs* /var/tmp/${restore_dir}*";
	system ($rm_cmd);
    }
}


sub in_prior_output($) {
    my $user = shift;

    return 0 if (!exists $opts{p});

    my $in;
    open ($in, $opts{p}) || die "unable to open $opts{p}";
    while (<$in>) {
	if (/finished $user at/i) {
	    close $in;
	    return 1;
	}
    }
    close $in;
    return 0;
}


sub finish_user($$) {
    my $in_user = shift;
    my $increment = shift;

    if (!defined $in_user) {
	die "\$in_user is not defined in finish_user";
    }

    if (!defined $increment) {
	die "\$increment is not defined in finish_user";
    }

    print "finished $in_user at ", `date`;

    $count++;
    if (exists ($opts{c}) && $increment) {
	if ($count > $opts{c}) {
	    print "\nStopped processing at requested count $opts{c}, exiting.\n";
	    email_summary_and_exit();
	}
    }
}


sub act_on_signal() {
    print "caught exit signal, cleaning up...\n";
    cleanup();
    exit();
}


sub check_time($) {
    my $t = shift;

    my ($min, $hour) = (localtime(time))[1,2];

    my $compare_time = $t;
    $compare_time =~ s/://;
    my $current_time = $hour.$min;

    return 1
      if ($current_time >= $compare_time);
    
    return 0;
}



sub email_summary_and_exit() {
    if (exists ($opts{e})) {
	if (exists $opts{p}) {
	    print "\nsending compressed email summary...\n";
	    my $output_base = basename ($opts{p}, ());
	    my $output_dirname = dirname ($opts{p});

	    my $gz_result = 0;

	    my $datestamp = `date +%g%m%d%H%M%S`;
	    chomp $datestamp;

	    my $datestamp_file = $output_base . "_" . $datestamp . ".gz";

	    my $gz_cmd = "gzip -c $opts{p} > ${output_dirname}/${datestamp_file}";
	    print $gz_cmd . "\n";
	    $gz_result = 1
	      if (system ($gz_cmd));

	    my $uu = `uuencode ${output_dirname}/${datestamp_file} ${datestamp_file}`
	      unless ($gz_result);

	    open (MAIL, "|mail -s \"backup2archive stopped `hostname`\" $mail_to");
	    unless ($gz_result) {
		print MAIL "The output file is attached.\n\n";
		print MAIL $uu;
	    }
	    close (MAIL);
	}
    }

    exit;
}
