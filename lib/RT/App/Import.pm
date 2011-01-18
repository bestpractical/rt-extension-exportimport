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

sub run {
    my $proto = shift;

    my @args = @_;
    return $proto->help unless @args;

    my %args;

    require Getopt::Long;
    Getopt::Long::GetOptionsFromArray( \@args, \%args, @OPTIONS );

    my $self = $proto->new( %args );
    $proto->import;

    return 0;
}

sub input { return $_[0]->{'input'} }

sub import {
}

1;
