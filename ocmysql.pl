#!/bin/env perl
package ocmysql;

use strict;
use warnings;
use DBI;
use Getopt::Std;
use Data::Types qw(:all );

#my $conf='/home/tools/ocmysql/ocmysql.conf';

$Getopt::Std::STANDARD_HELP_VERSION=1;
our (@psList, $dbh);


sub HELP_MESSAGE() {
    print << "EOF";

Oh Crap, MySQL!
Usage: $0 -h <HOST> [-s] [-q <DB>] [-c <CONFIG>]

Options
 -h      hostname or IP of MySQL server (required)
 -s      summary of connections and states on a per-user basis (default)
 -q      summary of queries for a specific database
 -c      config file (default /home/tools/ocmysql/ocmysql.conf)
 --help  shows this help (works with no other options)
EOF
}


sub querySum() {
    my $db = $_[0];
}



sub getConnSum() {
    my ($row, %row, %states, $state, $found, $connStatus, $sleeping, $db, $user, $avgTime, %connections);

    # Tally process states per user
    # TODO: output
    #   USER: 
    
    print '+=============================================================================================================+
| Database               | User                   | Total Conns | Running | Stalled | Idle | Other | Avg Time |
+-------------------------------------------------------------------------------------------------------------+
';
    
    # Ugly, but gives a way to lump connection states into simpler categories
    %states = (
               running => [
                           "After create",
                           "Analyzing",
                           "checking permissions",
                           "Checking table",
                           "cleaning up",
                           "closing tables",
                           "converting HEAP to MyISAM",
                           "copy to tmp table",
                           "Copying to group table",
                           "Copying to tmp table",
                           "Copying to tmp table on disk",
                           "Creating index",
                           "Creating sort index",
                           "creating table",
                           "Creating tmp table",
                           "deleting from main table",
                           "deleting from reference tables",
                           "discard_or_import_tablespace",
                           "end",
                           "executing",
                           "Execution of init_command",
                           "freeing items",
                           "Flushing tables",
                           "FULLTEXT initialization",
                           "init",
                           "preparing",
                           "query end",
                           "Reading from net",
                           "Removing duplicates",
                           "removing tmp table",
                           "rename",
                           "rename result table",
                           "Repair by sorting",
                           "Repair done",
                           "Repair with keycache",
                           "Rolling back",
                           "Saving state",
                           "Searching rows for update",
                           "Sending data",
                           "setup",
                           "Sorting for group",
                           "Sorting for order",
                           "Sorting index",
                           "Sorting result",
                           "statistics",
                           "Updating",
                           "updating",
                           "updating main table",
                           "updating reference tables",
                           "Writing to net",
                           "sending cached result to client",
                           "storing result in query cache",
                           "invalidating query cache entries",
                           "checking query cache for query",
                           "checking privileges on cached query",
                           "logging slow query"
                          ],
               stalled => [
                           "Locked",
                           "Opening tables",
                           "Opening table",
                           "Reopen tables",
                           "User lock",
                           "Waiting for tables",
                           "Waiting for table",
                           "Waiting for release of readlock",
                           "Waiting on cond",
                           "Waiting to get readlock",
                           "Waiting for INSERT"
                          ]
              );
    

    
    # Group connection states per DB and user
    for $row (@psList) {        
        # Ensure we have a connection we care about, specifically excluding delayed inserts
        if (defined $row->{'user'} && defined $row->{'db'} && $row->{'user'} ne 'DELAYED') {
            # Initialization of the general states
            if (!defined $connections{$row->{'db'}}{$row->{'user'}}{'running'}) {
                $connections{$row->{'db'}}{$row->{'user'}}{'running'} = 0;
            }

            if (!defined $connections{$row->{'db'}}{$row->{'user'}}{'stalled'}) {
                $connections{$row->{'db'}}{$row->{'user'}}{'stalled'} = 0;
            }

            if (!defined $connections{$row->{'db'}}{$row->{'user'}}{'sleep'}) {
                $connections{$row->{'db'}}{$row->{'user'}}{'sleep'} = 0;
            }

            if (!defined $connections{$row->{'db'}}{$row->{'user'}}{'other'}) {
                $connections{$row->{'db'}}{$row->{'user'}}{'other'} = 0;
            }

            if (!defined $connections{$row->{'db'}}{$row->{'user'}}{'total'}) {
                $connections{$row->{'db'}}{$row->{'user'}}{'total'} = 0;
            }
            
            if (!defined $connections{$row->{'db'}}{$row->{'user'}}{'totalTime'}) {
                $connections{$row->{'db'}}{$row->{'user'}}{'totalTime'} = 0;
            }

            if (!defined $connections{$row->{'db'}}{$row->{'user'}}{'size'}) {
                $connections{$row->{'db'}}{$row->{'user'}}{'size'} = 0;
            }

            if ($row->{'command'} eq 'Sleep') {
                # Count up the sleeping connections
                $connections{$row->{'db'}}{$row->{'user'}}{'sleep'} += 1;
            } elsif (defined $row->{'state'}) {
                # Determine what state it's in since it's not sleeping
                $found = 0;
                for $state (@{$states{'running'}}) {
                    if ($row->{'state'} eq $state) {
                        $connections{$row->{'db'}}{$row->{'user'}}{'running'} = $connections{$row->{'db'}}{$row->{'user'}}{'running'} + 1;
                        $found = 1;
                    }
                }
                if ($found != 1) {
                    for $state (@{$states{'stalled'}}) {
                        if ($row->{'state'} eq $state) {
                            $connections{$row->{'db'}}{$row->{'user'}}{'stalled'} = $connections{$row->{'db'}}{$row->{'user'}}{'stalled'} + 1;
                            $found = 1;
                        }
                    }
                }
                if ($found != 1) {
                    $connections{$row->{'db'}}{$row->{'user'}}{'other'} = $connections{$row->{'db'}}{$row->{'user'}}{'other'} + 1;
                    $found = 1;
                }    
            }
            
            $connections{$row->{'db'}}{$row->{'user'}}{'total'} += 1;
            $connections{$row->{'db'}}{$row->{'user'}}{'totalTime'} += $row->{'time'};
            $connections{$row->{'db'}}{$row->{'user'}}{'size'} += 1;
        } elsif (!defined $row->{'db'}) {
            if (!exists $connections{'none'}) {
                $connections{'none'} = { $row->{'user'} => {'running' => 0} };
                $connections{'none'} = { $row->{'user'} => {'stalled' => 0} };
                $connections{'none'} = { $row->{'user'} => {'sleep' => 0} };
                $connections{'none'} = { $row->{'user'} => {'other' => 0} };
                $connections{'none'} = { $row->{'user'} => {'total' => 0} };
                $connections{'none'} = { $row->{'user'} => {'totalTime' => 0} };
                $connections{'none'} = { $row->{'user'} => {'size' => 0} };
            }

            if (!defined $connections{'none'}{$row->{'user'}}{'running'}) {
                $connections{'none'}{$row->{'user'}}{'running'} = 0;
            }
            
            if (!defined $connections{'none'}{$row->{'user'}}{'stalled'}) {
                $connections{'none'}{$row->{'user'}}{'stalled'} = 0;
            }
                        
            if (!defined $connections{'none'}{$row->{'user'}}{'sleep'}) {
                $connections{'none'}{$row->{'user'}}{'sleep'} = 0;
            }
            
            if (!defined $connections{'none'}{$row->{'user'}}{'other'}) {
                $connections{'none'}{$row->{'user'}}{'other'} = 0;
            }
            
            if (!defined $connections{'none'}{$row->{'user'}}{'total'}) {
                $connections{'none'}{$row->{'user'}}{'total'} = 0;
            }
            
            if (!defined $connections{'none'}{$row->{'user'}}{'totalTime'}) {
                $connections{'none'}{$row->{'user'}}{'totalTime'} = 0;
            }
            
            if (!defined $connections{'none'}{$row->{'user'}}{'size'}) {
                $connections{'none'}{$row->{'user'}}{'size'} = 0;
            }
            
            $connections{'none'}{$row->{'user'}}{'other'} += 1;
            $connections{'none'}{$row->{'user'}}{'total'} += 1;
            $connections{'none'}{$row->{'user'}}{'size'} += 1;
            if (! defined $row->{'time'}) {
                $row->{'time'} = 0;
            }
            $connections{'none'}{$row->{'user'}}{'totalTime'} += $row->{'time'};
        }
    }
    
    for $db (keys %connections) {
        for $user (keys %{$connections{$db}}) {
            if (!defined $connections{$db}{$user}{'totalTime'}) {
                $connections{$db}{$user}{'totalTime'} = 0;
            }
            
            if (!defined $connections{$db}{$user}{'avgTime'}) {
                $connections{$db}{$user}{'avgTime'} = 0;
            }
                        
            if ($connections{$db}{$user}{'totalTime'} > 0 && $connections{$db}{$user}{'size'} > 0) {
                $connections{$db}{$user}{'avgTime'} = $connections{$db}{$user}{'totalTime'} / $connections{$db}{$user}{'size'};
            }
        }
    }
    
    

    for $db (keys %connections) {
        for $user (keys %{$connections{$db}}) {
            printf("| %22s | %22s | %11d | %7d | %7d | %4d | %5d | %8d |\n", $db, $user, $connections{$db}{$user}{'total'}, $connections{$db}{$user}{'running'}, $connections{$db}{$user}{'stalled'}, $connections{$db}{$user}{'sleep'}, $connections{$db}{$user}{'other'}, $connections{$db}{$user}{'avgTime'});

        }
    }
    
    print '+=============================================================================================================+' . "\n";
}


# Gathers connection usage info
sub getConnStats() {
    my ($sth, $maxConn, $maxUsedConn, $usedConn, $availConn, @row, %connStats);
    
    # Get the global max connections
    $sth = $dbh->prepare("SHOW GLOBAL VARIABLES LIKE 'max_connections'");
    $sth->execute();
    die $sth->errstr if $sth->err;
    
    while (@row = $sth->fetchrow_array()) {
        $maxConn = $row[1];
    }
    
    # Get the global max used connetions
    $sth = $dbh->prepare("SHOW STATUS LIKE 'max_used_connections'");
    $sth->execute();
    die $sth->errstr if $sth->err;
        
    while (@row = $sth->fetchrow_array()) {
        $maxUsedConn = $row[1];
    }
    
    # Used connections based on size of process list array
    $usedConn = @psList;
    $availConn = $maxConn - $usedConn;
    %connStats = (
                    'usedConn'    => $usedConn,
                    'availConn'   => $availConn,
                    'maxUsedConn' => $maxUsedConn,
                    'maxConn'     => $maxConn
                  );
    
    printf("Current Used / Avail Connections:  %4u / %4u\n", $connStats{'usedConn'}, $connStats{'availConn'});
    printf("Maximum Used / Global Connections: %4u / %4u\n", $connStats{'maxUsedConn'}, $connStats{'maxConn'}); 
}


# Fetches the process list and populates the global @psList as an array of hashes
sub getProcList() {
    my ($sth, @row);
    
    # Fetch the process list
    $sth = $dbh->prepare("SHOW FULL PROCESSLIST");
    $sth->execute();

    # Store the process list for use later
    while (@row = $sth->fetchrow_array()) {
        push @psList, { 
                        'pid'     => $row[0],
                        'user'    => $row[1],
                        'db'      => $row[3],
                        'command' => $row[4],
                        'time'    => $row[5],
                        'state'   => $row[6],
                        'info'    => $row[7]
                       };
    }
}


sub main() {
    my $conf='ocmysql.conf';
    my ($username, $password, $option, $value, %opts, @row);
    
    getopts('sh:q:c:', \%opts);

    # Get the config (contains auth info)
    if (defined $opts{c}) {
        $conf = $opts{c};  
    } else { 
        $conf = 'ocmysql.conf'; 
    }

    open CONF, $conf or die $!;
    while (<CONF>) {
        chomp;
        if (/^(username|password)=.*/) {
            ($option, $value) = split /=/;
            if ($option eq "username") {
                $username = $value;
            } elsif ($option eq "password") {
                $password = $value;
            }
        }
    }
    close CONF;
    
    # Check to see if a host was supplied
    if (!defined $opts{h}) { 
        print "Hostname or IP required\n"; 
        HELP_MESSAGE();
        exit; 
    }
    
    # Get our connection to the database
    my $dsn = "DBI:mysql:database=mysql;host=$opts{h}";
    $dbh = DBI->connect($dsn, $username, $password, {'RaiseError' => 1}) or die "Couldn't connect to database: " . DBI->errstr;
    
    # Proceed to with the business
    getProcList();
    
    if (defined $opts{q}) {
        #querySum($opts{q});
    }
    if (defined $opts{s} || !defined $opts{q}) {
        print "Oh crap, MySQL!\n\nHost: $opts{h}\n";
        getConnStats();
        getConnSum();
    }
    
    # Close the connection to the DB since we don't need it any more
    $dbh->disconnect();
}


main();
