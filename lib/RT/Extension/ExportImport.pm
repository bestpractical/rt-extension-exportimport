use 5.008;
use strict;
use warnings;

package RT::Extension::ExportImport;

our $VERSION = '0.01';

=head1 NAME

RT::Extension::Export - export tickets 

=head1 DECRIPTION

=cut

use File::Spec;
use Text::CSV;

sub new {
    my $proto = shift;
    my $self = bless { @_ }, ref($proto) || $proto;
    return $self->init;
}

sub init { return shift }

sub export {
    my $self = shift;
    my %args = (
        class => undef,
        alias => undef,
        query => undef,
        binds => [],
        @_
    );

    my @fields = $self->fields( class => $args{'class'} );

    my $seen = $self->{'seen'}{ $args{'class'} } ||= {};

    my ($fh, $csv) = $self->csv( %args );

    my $query = 'SELECT '. join(', ', map "$args{'alias'}.$_", @fields)
        .' FROM '. $args{'query'};
    print "$query\n";
    print "\tbindings: ". join(', ', map "'$_'", @{ $args{'binds'} }) . "\n"
        if @{ $args{'binds'} };

    my $sth = $RT::Handle->SimpleQuery( $query, @{ $args{'binds'} } );
    unless ( $sth ) {
        die "Couldn't execute the query: ". $sth->error_message;
    }

    my $counter = 0;
    while ( my $row = $sth->fetchrow_arrayref ) {
        next if exists $seen->{ $row->[0] };
        $seen->{ $row->[0] } = undef;
        $counter++;

        $csv->print($fh, $row);
    }
    print "Exported $counter rows\n";
    return $counter;
}

sub csv {
    my $self = shift;
    my %args = (
        class => undef,
        @_
    );

    my $name = lc $args{'class'};
    $name =~ s/:/_/g;
    my $path = File::Spec->catfile( $self->output, $name . '.csv' );
    my $exists = -e $path;

    open my $fh, '>>:raw', $path
        or die "Couldn't open '$path': $!";

    my $csv = Text::CSV->new ( { binary => 1 } );

    unless ( $exists ) {
        print $fh '# RT '. $RT::VERSION .' '. $args{'class'} ."\n";
        $csv->print( $fh, [ $self->fields( class => $args{'class'} ) ] );
    }
    return ($fh, $csv);
}

sub fields {
    my $self = shift;
    my %args = (@_);

    return sort { $a eq 'id'? -1 : $a cmp $b } $RT::Handle->Fields( $self->table( %args ) );
}

sub table {
    my $self = shift;
    my %args = (@_);
    return $args{'class'}->new( $RT::SystemUser )->Table;
}

=head1 AUTHOR

Ruslan Zakirov E<lt>ruz@bestpractical.comE<gt>

=head1 LICENSE

Under the same terms as perl itself.

=cut

1;
