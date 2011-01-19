use strict;
use warnings;

package RT::App::Export;
use base 'RT::Extension::ExportImport';

=head1 NAME

RT::App::Export - export tickets 

=head1 DECRIPTION

=cut

use File::Spec;
use Text::CSV;

sub init {
    my $self = shift;

    $self->{'output'} ||= 'rt_export';

    if ( -e $self->{'output'} ) {
        die $self->{'output'} . " is not a directory" unless -d $self->{'output'};
    } else {
        require File::Path;
        File::Path::make_path( $self->{'output'} );
    }

    return $self;
}

my @OPTIONS = ('tickets=s', 'output=s');

sub run {
    my $proto = shift;

    my @args = @_;
    return $proto->help unless @args;

    my %args;

    require Getopt::Long;
    Getopt::Long::GetOptionsFromArray( \@args, \%args, @OPTIONS );

    my $self = $proto->new( %args );

    if ( exists $args{'tickets'} ) {
        $args{'tickets'} = [ split /-/, $args{'tickets'} ];
        $self->export_tickets( %args );
    }
    return 0;
}

sub output { $_[0]->{'output'} }

our %RELATION = (
    'RT::Ticket' => [
        'role_groups',
        'ocfv',
    ],
    'RT::CachedGroupMember' => [
        'cgm_self',
    ],
    'RT::Transaction' => [
        'transaction_values',
        'ocfv',
    ],
    '*' => [
        'common_users',
        'transactions',
        'links',
        'attributes',
    ],
);

our %SIMPLE_RELATION = (
    'RT::Group' => [
        { class => 'RT::Principal', alias => 'p', columns => { id => 'id' }, follow => 0 },
        { class => 'RT::GroupMember', alias => 'gm', columns => { GroupId => 'id' }, },
        { class => 'RT::CachedGroupMember', alias => 'cgm', columns => { GroupId => 'id' }, },
    ],
    'RT::User' => [
        { class => 'RT::Principal', alias => 'p', columns => { id => 'id' }, follow => 0 },
    ],
    'RT::Principal' => [
        { class => 'RT::Group', alias => 'g', columns => { id => 'id' } },
        { class => 'RT::User', alias => 'u', columns => { id => 'id' } },
    ],
    'RT::GroupMember' => [
        { class => 'RT::Group', alias => 'g', columns => { id => 'GroupId' } },
        { class => 'RT::Principal', alias => 'p', columns => { id => 'MemberId' } },
    ],
    'RT::CachedGroupMember' => [
        { class => 'RT::Group', alias => 'g', columns => { id => 'GroupId' } },
        { class => 'RT::Principal', alias => 'p', columns => { id => 'MemberId' } },
    ],
    'RT::Transaction' => [
        { class => 'RT::Attachment', alias => 'a', columns => { TransactionId => 'id' } },
    ],
);

sub push_collection {
    my $self = shift;
    my %args = (
        class => undef,
        query => undef,
        alias => undef,
        binds => [],
        follow => 1,
        @_
    );

    push @{ $self->{'collections'} ||= [] }, \%args;

    return if $self->{'pushing_collection'};
    local $self->{'pushing_collection'} = 1;

    while ( my $entry = shift @{ $self->{'collections'} } ) {
        my $count = $self->export( %$entry ) or next;

        next unless $entry->{'follow'};

        foreach my $simple ( @{ $SIMPLE_RELATION{ $entry->{'class'} } || [] } ) {
            my ($class, $alias) = @{ $simple }{'class', 'alias'};

            my $columns = delete $simple->{'columns'};
            while ( my ($lhs, $rhs) = each %$columns ) {
                my $query =
                    $self->table( class => $class ) .' '. $alias
                    ." WHERE $alias.$lhs IN ("
                    .'    SELECT '. $entry->{alias} .'.'. $rhs .' FROM '. $entry->{query}
                    .')'
                ;
                $self->push_collection(
                    %$simple,
                    query => $query,
                    binds => $entry->{'binds'},
                );
            }
        }
        foreach my $name (
            @{ $RELATION{ $entry->{'class'} } || [] },
            @{ $RELATION{'*'} }
        ) {
            my $method = 'follow_'. $name;
            $self->$method( %$entry );
        }
    }
}

sub export_tickets {
    my $self = shift;
    my %args = @_;

    my (@binds, @parts);
    my $range = $args{'tickets'};
    if ( $range->[0] ) {
        push @parts, 't.id >= ?';
        push @binds, $range->[0];
    }
    if ( $range->[1] ) {
        push @parts, 't.id < ?';
        push @binds, $range->[1];
    }

    my $query = "Tickets t";
    $query .= ' WHERE '. join(' AND ', @parts)
        if @parts;

    return $self->push_collection(
        class => 'RT::Ticket',
        query => $query,
        alias => 't',
        binds => \@binds,
    );
}

sub follow_role_groups {
    my $self = shift;
    my %args = (@_);

    my $query = "
        Groups g
        WHERE g.Domain = ? AND g.Instance IN (
            SELECT $args{alias}.id FROM $args{query}
        )
    ";
    my @binds = ( $args{'class'} .'-Role', @{ $args{'binds'} } );
    return $self->push_collection(
        class => 'RT::Group',
        alias => 'g',
        query => $query,
        binds => \@binds,
    );
}

sub follow_cgm_self {
    my $self = shift;
    my %args = (@_);

    my $query = "
        CachedGroupMembers cgm
        WHERE cgm.Via != cgm.id AND cgm.id IN (
            SELECT $args{alias}.Via FROM $args{query}
        )
    ";
    return $self->push_collection(
        class => 'RT::CachedGroupMember',
        alias => 'cgm',
        query => $query,
        binds => $args{'binds'},
    );
}

sub follow_common_users {
    my $self = shift;
    my %args = (@_);

    # common user reference
    foreach my $reference ( qw(Creator LastUpdatedBy) ) {
        next unless $args{'class'}->_ClassAccessible->{ $reference };

        my $query = "
            Users u
            WHERE u.id IN (
                SELECT $args{alias}.$reference FROM $args{query}
            )
        ";
        $self->push_collection(
            class => 'RT::User',
            alias => 'u',
            query => $query,
            binds => $args{'binds'},
        );
    }
}

sub follow_transactions {
    my $self = shift;
    my %args = (@_);

    my $query = "
        Transactions txn
        WHERE txn.ObjectType = ? AND txn.ObjectId IN (
            SELECT $args{alias}.id FROM $args{query}
        )
    ";
    return $self->push_collection(
        class => 'RT::Transaction',
        alias => 'txn',
        query => $query,
        binds => [ $args{'class'}, @{ $args{'binds'} } ],
    );
}

sub follow_transaction_values {
    my $self = shift;
    my %args = (@_);

    # custom field
    foreach my $field (qw(OldValue NewValue)) {
        my $query = "
            ObjectCustomFieldValues ocfv
            WHERE ocfv.id IN (
                SELECT $args{alias}.$field FROM $args{query}
                AND $args{alias}.Type = ?
            )
        ";
        $self->push_collection(
            class => 'RT::ObjectCustomFieldValue',
            alias => 'ocfv',
            query => $query,
            binds => [ @{ $args{'binds'} }, 'CustomField' ],
        );
    }

    # owner
    foreach my $field (qw(OldValue NewValue)) {
        my $query = "
            Principals p
            WHERE p.id IN (
                SELECT $args{alias}.$field FROM $args{query}
                AND (
                    $args{alias}.Type IN (?, ?, ?, ?, ?)
                    OR ($args{alias}.Type = ? AND $args{alias}.Field = ?)
                )
            )
        ";
        $self->push_collection(
            class => 'RT::Principals',
            alias => 'p',
            query => $query,
            binds => [ @{ $args{'binds'} }, qw(Untake Take Force Steal Give Set Owner) ],
        );
    }

    # watchers
    foreach my $field (qw(OldValue NewValue)) {
        my $query = "
            Principals p
            WHERE p.id IN (
                SELECT $args{alias}.$field FROM $args{query}
                AND $args{alias}.Type IN (?, ?)
            )
        ";
        $self->push_collection(
            class => 'RT::Principal',
            alias => 'p',
            query => $query,
            binds => [ @{ $args{'binds'} }, qw(AddWatcher DelWatcher) ],
        );
    }

    # links
    foreach my $field (qw(OldValue NewValue)) {
        my $query = "
            Links l
            WHERE l.id IN (
                SELECT $args{alias}.$field FROM $args{query}
                AND $args{alias}.Type IN (?, ?)
            )
        ";
        $self->push_collection(
            class => 'RT::Link',
            alias => 'l',
            query => $query,
            binds => [ @{ $args{'binds'} }, qw(AddLink DeleteLink) ],
        );
    }
}

sub follow_ocfv {
    my $self = shift;
    my %args = (@_);

    my $query = "
        ObjectCustomFieldValues ocfv
        WHERE ocfv.ObjectType = ? AND ocfv.ObjectId IN (
            SELECT $args{alias}.id FROM $args{query}
        )
    ";
    return $self->push_collection(
        class => 'RT::ObjectCustomFieldValue',
        alias => 'ocfv',
        query => $query,
        binds => [ $args{'class'}, @{ $args{'binds'} } ],
    );
}


sub follow_attributes {
    my $self = shift;
    my %args = (@_);

    my $query = "
        Attributes attr
        WHERE attr.ObjectType = ? AND attr.ObjectId IN (
            SELECT $args{alias}.id FROM $args{query}
        )
    ";
    return $self->push_collection(
        class => 'RT::Attribute',
        alias => 'attr',
        query => $query,
        binds => [ $args{'class'}, @{ $args{'binds'} } ],
    );
}

sub follow_links {
    my $self = shift;
    my %args = (@_);

    foreach my $direction (qw(Base Target)) {
        my ($query, $binds);
        if ( $args{'class'} eq 'RT::Ticket' ) {
            $query = "
                Links l
                WHERE l.Local$direction IN (
                    SELECT $args{alias}.id FROM $args{query}
                )
            ";
            $binds = $args{'binds'};
        } else {
            my $prefix = RT::URI::fsck_com_rt->LocalURIPrefix .'/'. $args{'class'} .'/';
            $query = "
                Links l
                WHERE l.$direction IN (
                    SELECT CONCAT(?, $args{alias}.id) FROM $args{query}
                )
            ";
            $binds = [ $prefix, @{ $args{'binds'} } ];
        }
        $self->push_collection(
            class => 'RT::Link',
            alias => 'l',
            query => $query,
            binds => $binds,
        );
    }
}

=head1 AUTHOR

Ruslan Zakirov E<lt>ruz@bestpractical.comE<gt>

=head1 LICENSE

Under the same terms as perl itself.

=cut

1;

