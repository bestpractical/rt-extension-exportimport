use strict;
use warnings;

package RT::App::Import;
use base 'RT::Extension::ExportImport';

=head1 NAME

RT::App::Import - import tickets 

=head1 DECRIPTION

=cut

sub init {
    my $self = shift;

    unless ( $self->{'input'} ) {
        die "--input is a mandatory argument";
    }
    unless ( -d $self->{'input'} ) {
        die $self->{'input'} . " is not a directory";
    }

    return $self;
}

our @OPTIONS = ('input=s', 'to-version=s', 'debug!');

sub run {
    my $proto = shift;

    my @args = @_;
    return $proto->help unless @args;

    my %args;

    require Getopt::Long;
    Getopt::Long::GetOptionsFromArray( \@args, \%args, @OPTIONS );

    my $self = $proto->new( %args );
    $self->import_dir;

    return 0;
}

sub input { return $_[0]->{'input'} }

sub import_dir {
    my $self = shift;

    my $blob = File::Spec->catfile( $self->input, '*.csv' );
    my @files = glob $blob;

    foreach my $file ( @files ) {
        $self->debug("About to process '$file'");

        open my $fh, '<', $file
            or die "Couldn't open '$file': $!";
        my $csv = $self->csv;

        my ($token, $version, $class) = @{ $csv->getline( $fh ) || [] };
        unless ( $token eq 'rt-export' ) {
            die "Doesn't look like an export from RT";
        }

        $self->debug("File contains $class records exported from RT $version");

        my $fields = $csv->getline( $fh );
        $self->debug("Fields ". join(', ', "'$_'", @$fields));

        $self->import_class(
            file    => $file,
            handle  => $fh,
            csv     => $csv,
            version => $version,
            class   => $class,
            table   => $self->table( class => $class ),
            fields  => $fields,
        );

    }
}

our %IMMUTABLE = map { $_ => 1 } qw(
    RT::Transaction
    RT::Attachment
    RT::ObjectCustomFieldValue
    RT::GroupMember
    RT::CachedGroupMember
);

sub import_class {
    my $self = shift;
    my %args = @_;

    my %current_field = map { lc($_) => 1 } $RT::Handle->Fields( $args{'table'} );

    while ( my $row = $args{'csv'}->getline( $args{'handle'} ) ) {
        my %row = map { lc($_) => shift(@$row) } @{ $args{'fields'} };

        delete $row{$_} foreach grep !$current_field{ $_ }, keys %row;

        $self->import_record( %args, row => \%row );
    }

}

sub import_record {
    my $self = shift;
    my %args = @_;

    my $query = 'SELECT id FROM '. $args{'table'} .' WHERE id = ?';

    my $sth = $RT::Handle->SimpleQuery( $query, $args{'row'}{'id'} );
    unless ( $sth ) {
        die "Couldn't execute the query: ". $sth->error_message;
    }

    unless ( $sth->fetchrow_array ) {

        $self->debug( "Inserting $args{'class'} #". $args{'row'}{'id'} );

        my @fields = keys %{ $args{'row'} };
        my $query =
            'INSERT INTO '. $args{'table'} .'('. join(', ', @fields ) .')'
            .' VALUES ('. join(', ', ('?') x @fields ) .')';

        my $sth = $RT::Handle->SimpleQuery( $query, map $args{'row'}{$_}, @fields );
        unless ( $sth ) {
            die "Couldn't execute the query: ". $sth->error_message;
        }
    } else {
        if ( $IMMUTABLE{ $args{'class'} } ) {
            $self->debug( "$args{'class'} #". $args{'row'}{'id'} .' exists and record is immutable. Skipping' );
            return;
        }

        $self->debug( "Updating $args{'class'} #". $args{'row'}{'id'} );

        my @fields = grep $_ ne 'id', keys %{ $args{'row'} };
        my $query =
            'UPDATE '. $args{'table'} .' SET '. join(' AND ', map "$_ = ?", @fields ) .''
            .' WHERE id = ?';

        my $sth = $RT::Handle->SimpleQuery( $query, map $args{'row'}{$_}, @fields, $args{'row'}{'id'} );
        unless ( $sth ) {
            die "Couldn't execute the query: ". $sth->error_message;
        }
    }
}

1;
