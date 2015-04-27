#!/usr/bin/perl -w
#

use Getopt::Std;
use File::Find;
use File::Copy;
use strict;
use Data::Dumper;
use IPC::Run qw(start);
#use LWP::Simple;

sub print_usage();
sub wanted;
sub in_prior_output($);

my $recovery_user;
my $recovery = "_recovery_";
my $restored = "_restored_";
my $restore_dir = "Import2015";
my $z_admin_pass="pass";
my $z_ldap_host="mldap01.domain.org";

my %opts;

getopts('nru:f:p:zc:', \%opts);

if (exists $opts{c}) {
    if ($opts{c} !~ /^\d+$/) {
	print "-c must be a number.  Exiting.";
	exit;
    }
}

print "starting ";
print $opts{c}, " limited "
  if (exists $opts{c});
print " run at ", `date`;

print "-c used, limiting processing to $opts{c} accounts\n"
  if (exists $opts{c});

print "-n used, no changes will be made\n"
  if (exists $opts{n});

if (exists $opts{p}) {
    if (defined $opts{p}) {
	print "-p used, file $opts{p} will be used to identify users processed prior\n";
    } else {
	print "-p requires a valid file name\n";
    }
}

unless (exists $opts{u} || exists $opts{f} || exists $opts{z}) {
    print_usage();
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
	exit;
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

for my $user (@users) {
    if (in_prior_output($user)) {
	print "$user processed prior, skipping.\n";
	next;
    }

    print "\n\n*** starting on $user ($count";
    print "/" . $opts{c}
      if (exists $opts{c});
    print ") at ", `date`;
    my $archive;



    ## find archive account to which to restore
    print "finding archive account...\n";
    $archive = `zmprov ga $user zimbraarchiveaccount|grep -i zimbraArchiveAccount| awk '{print \$2}'`;
    chomp $archive;

    $archive = $restored . $archive
      if (exists $opts{r});

    print "restoring into archive: $archive\n";

    $recovery_user = $recovery . $user;
    print "\nrestoring $recovery_user...\n";


    ## restore primary account with --skipDeletes to _recovery_ account
    my $restore_cmd = "zmrestore -d --skipDeletes -a $user -restoreToTime 20150413.121500 -t /opt/zimbra/backup1 -ca -pre " . $recovery;
    print "$restore_cmd\n";
    unless (exists $opts{n}) {
    	# if (system($restore_cmd)) {
	#     print "\nrestore failed, exiting\n";
    	#     cleanup();
    	#     exit;
    	# }

    	if (system($restore_cmd)) {
    	    print "\nrestore failed, trying with --ignoreRedoErrors\n";
    	    my $restore_ignoreredo_cmd = "zmrestore -d --skipDeletes --ignoreRedoErrors -a $user -restoreToTime 20150413.123000 -t /opt/zimbra/backup1 -ca -pre " . $recovery;
    	    print $restore_ignoreredo_cmd . "\n";
    	    if (system ($restore_ignoreredo_cmd)) {
    		print "\nrestore failed with --ignoreRedoErrors, giving up\n";
    		cleanup();
    		exit;
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



    ## export mail from 4/10 to 4/13
    print "exporting mail from 4/10/15 to 4/13/15...\n";

#    my $export_cmd = "zmmailbox -z -m $recovery_user gru '//?fmt=tgz&query=under:/ after:\"4/9/15\" AND before:\"4/14/15\"' > /var/tmp/msgs.tgz 2>&1";
#    print $export_cmd . "\n";
    unless (exists $opts{n}) {
	##   system won't fly as stderr must be captured to tell the
	##   difference between a failure because no mail was exported for
	##   that time range and a failure for another reason
    	# if (system ($export_cmd)) {
    	#     print "export failed, exiting.\n";
    	#     cleanup();
    	#     exit;
    	# }

	my @cmd = qw/zmmailbox -z -m/;
	push @cmd, $recovery_user;
	push @cmd, "gru";
        push @cmd, "'//?fmt=tgz&query=under:/ after:\"4/9/15\" AND before:\"4/14/15\"'";

	print join (' ', @cmd, "\n");

	my $h = start \@cmd, '>', '/var/tmp/msgs.tgz', '2>pipe', \*ERR;
	my $rc = finish $h;

	my @err;
	while (<ERR>) {
	    push @err, $_;
	}
	close ERR;

	my $err = join ' ', @err;

	if ($err =~ /status=204.  No data found/) {
	    print "$user: no data to import, skipping.\n";
	    cleanup();
	    $count++;
	    if ($count > $opts{c}) {
		print "\nStopped processing at requested count $opts{c}, exiting.\n";
		last;
	    }
	    print "finished $user at ", `date`;
	    next;
	}

	if (!$rc) {
	    print $err;
    	    print "export failed, exiting.\n";
    	    cleanup();
    	    exit;
    	}
    }



    ## decompress exported files, move to $restore_dir and compress for input via pru
    
    my $decompress_cmd = "(mkdir /var/tmp/$restore_dir && mkdir /var/tmp/msgs && cd /var/tmp/msgs && tar xfz ../msgs.tgz)";
    print $decompress_cmd . "\n";
    unless (exists $opts{n}) {
    	if (system ($decompress_cmd)) {
    	    print "decompress messages failed, exiting.\n";
    	    cleanup();
    	    exit;
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
    	    exit;
    	}
    }


    # print "\n";
    # print "importing messages to $archive\n";
    # my $import_cmd = "zmmailbox -z -m $archive pru \"//?fmt=tgz&subfolder=$restore_dir\" /var/tmp/$restore_dir.tgz";
    # print $import_cmd . "\n";
    # unless (exists $opts{n}) {
    # 	if (system ($import_cmd)) {
    # 	    print "import failed, exiting.\n";
    # 	    cleanup();
    # 	    exit;
    # 	}
    # }


    cleanup();

    if (exists $opts{n}) {
	# print a slightly different message for a dry run so the script doesn't skip this user on future runs
	print "finished (dry) $user at ", `date`;
    } else {
	print "finished $user at ", `date`;
    }

    if (exists $opts{c}) {
	$count++;
	if ($count > $opts{c}) {
	    print "\nStopped processing at requested count $opts{c}, exiting.\n";
	    last;
	}
    }
}

print "finished run at ", `date`;

sub print_usage() {
    print "usage: $0 [-n] [-r] [-c <count>] [-p <previous output file>] \n";
    print "\t-u <user> | -f <user list file> | -z \n";
    print "\n";
    exit;
}


sub wanted {
    if ($File::Find::name =~ /\.eml$/) {
	if (!move ($File::Find::name, "/var/tmp/$restore_dir")) {
	    print "moving $File::Find::name failed, exiting\n";
	    cleanup();
	    exit;
	}
    }
}

sub cleanup {
    print "cleaning up...\n";
    print "removing $recovery_user...\n";
    unless (exists $opts{n}) {
    	my $remove_recovery_cmd = "zmprov da $recovery_user";
    	system ($remove_recovery_cmd);
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
