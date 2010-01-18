#!/usr/bin/perl

=head1 SYNOPSIS

perl convert.pl base-uri

base-uri should be the base URI used in constructing linked-data capable
instance URIs. It will be appended with fragments starting with '/', so it
should not end with a slash.

Example:

 perl convert.pl http://myrdf.us/mta/mnr

This will create RDF with URIs such as:

 http://myrdf.us/mta/mnr/stop/grand_central_terminal

=cut

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use Error qw(:try);

unless (@ARGV) {
	help();
	exit;
}

my ($lic, $base, $source);
my $result	= GetOptions(
				"base=s"	=> \$base,
				"license=s"	=> \$lic,
				"source=s"	=> \$source,
			);

my %args;
if ($base) {
	$args{base}	= $base;
} else {
	help();
	exit;
}

$args{license}	= $lic if ($lic);
$args{source}	= $source if ($source);

my $m		= MTA->new({%args});

$m->run();

exit;

sub help {
	print <<"END";
Usage: $0 -base uri -license url

The base URI is used in constructing linked-data capable instance URIs. It will
be appended with fragments starting with '/', so it should not end with a slash.

Example:

perl $0 -base http://myrdf.us/mta/mnr

This will create RDF with URIs such as:
http://myrdf.us/mta/mnr/stop/grand_central_terminal

END
}

################################################################################
################################################################################
################################################################################


package MTA;

use strict;
use warnings;
use base qw(Class::Accessor);

use Text::CSV;
use Data::Dumper;
use URI::Escape;
use Scalar::Util qw(reftype);

our %ROUTE_TYPES;
BEGIN {
	MTA->mk_accessors(qw(base license source optional_files));
	%ROUTE_TYPES	= (
		0	=> 'LightRail',
		1	=> 'Subway',
		2	=> 'Rail',
		3	=> 'Bus',
		4	=> 'Ferry',
		5	=> 'CableCar',
		6	=> 'Gondola',
		7	=> 'Funicular',
	);
}

sub run {
	my $self	= shift;
	$self->check_files();
	$self->init();
	$self->parse( 'calendar' );
	$self->parse( 'agency' );
	$self->parse( 'routes' );
	$self->parse( 'frequencies' );
	$self->parse( 'trips' );
	$self->parse( 'stops' );
	$self->parse( 'stop_times' );
	$self->finish_assertions();
	$self->emit_dataset();
}

sub init {
	my $self	= shift;
	my $base	= $self->base;
	printf(<<'END', $base);
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dc: <http://purl.org/dc/elements/1.1/> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix void: <http://rdfs.org/ns/void#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix : <http://myrdf.us/gtfs/vocab/> .

END
}

sub check_files {
	my $self	= shift;
	my @files;
	foreach my $f (qw(agency stops routes trips stop_times calendar)) {
		my $file	= "${f}.txt";
		unless (-r $file) {
			throw Error -text => "Missing required file $file";
		}
	}
	foreach my $f (qw(calendar_dates fare_attributes fare_rules shapes frequencies transfers)) {
		my $file	= "${f}.txt";
		if (-r $file) {
			push(@files, $file);
		}
	}
	$self->optional_files( @files );
}

sub parse {
	my $self	= shift;
	my $type	= shift;
	my $file	= "${type}.txt";
	return unless (-r $file);
	my $csv		= Text::CSV->new ( { binary => 1 } ) or die "Cannot use CSV: ".Text::CSV->error_diag ();
	open( my $fh, '<:utf8', $file ) or throw Error -text => $!;
	print "# ${type}\n";
	my $header	= $csv->getline( $fh );
	my @fields	= @$header;
	my %fields;
	my $col	= 0;
	foreach (@fields) {
		s/^\s+//;
		chomp;
		$fields{ $_ }	= $col++;
	}
	
	my $method	= "emit_$type";
	while (my $row = $csv->getline( $fh )) {
		my %row;
		foreach (@$row) {
			s/^\s+//;
			chomp;
		}
		@row{ @fields }	= @$row;
		$self->$method( %row );
	}
}

################################################################################

sub emit_agency {
	my $self	= shift;
	my %row		= @_;
	foreach (qw(agency_name agency_url agency_timezone)) {
		unless (exists $row{$_}) {
			throw Error -text => "Missing agency field $_";
		}
	}
	
	my $name	= $row{ 'agency_name' };
	my $url		= $row{ 'agency_url' };
	my $tz		= $row{ 'agency_timezone' };
	my $id		= $self->_make_id( agency_name => $name );
	
	my $uri		= sprintf('%s/agency/%s', $self->base, uri_escape($id));
	
	if (exists $row{ 'agency_id' }) {
		my $aid	= $row{ 'agency_id' };
		$self->{agency}{$aid}	= $uri;
	}
	
	print <<"END";
<$uri> a :Agency ;
	dc:title "$name" ;
	foaf:homepage <$url> ;
	:timezone "$tz" .

END
}

################################################################################

sub emit_calendar {
	my $self	= shift;
	my %row		= @_;
	foreach (qw(service_id monday tuesday wednesday thursday friday saturday sunday start_date end_date)) {
		unless (exists $row{$_}) {
			throw Error -text => "Missing calendar field $_";
		}
	}
	
	my $sid		= $row{ 'service_id' };
	my $mon		= _bool( $row{ 'monday' } );
	my $tue		= _bool( $row{ 'tuesday' } );
	my $wed		= _bool( $row{ 'wednesday' } );
	my $thu		= _bool( $row{ 'thursday' } );
	my $fri		= _bool( $row{ 'friday' } );
	my $sat		= _bool( $row{ 'saturday' } );
	my $sun		= _bool( $row{ 'sunday' } );
	my $start	= _date( $row{ 'start_date' } );
	my $end		= _date( $row{ 'end_date' } );
	
	my $uri		= sprintf('%s/service/%s', $self->base, uri_escape($sid));
	print <<"END";
<$uri> a :Service ;
	:start_date $start ;
	:end_date $end ;
	:monday $mon ;
	:tuesday $tue ;
	:wednesday $wed ;
	:thursday $thu ;
	:friday $fri ;
	:saturday $sat ;
	:sunday $sun ;
	.

END
}

################################################################################

sub emit_routes {
	my $self	= shift;
	my %row		= @_;
	foreach (qw(route_id route_long_name route_type)) {
		unless (exists $row{$_}) {
			throw Error -text => "Missing routes field $_";
		}
	}
	
	my $rid		= $row{ 'route_id' };
	my $short	= $row{ 'route_short_name' };
	my $long	= $row{ 'route_long_name' };
	my $type	= 0+$row{ 'route_type' };
	my $typeQName	= $ROUTE_TYPES{ $type };
	unless ($typeQName) {
		throw Error -text => "Unknown route type '$type'";
	}
	
	my $name	= $short || $long;
	my $id		= $self->_make_id( route_name => $name );
	
	my $uri		= sprintf('%s/route/%s', $self->base, uri_escape($id));
	$self->{routes}{$rid}	= $uri;
	
	print <<"END";
<$uri> a :Route ;
	dcterms:identifier "$rid" ;
	:route_type :$typeQName ;
	rdfs:label "$name" ;
END
	print qq[\tdc:title "$short" ;\n] if ($short);
	print qq[\tdc:description "$long" ;\n] if ($long);
	
	if (exists $row{ 'agency_id' }) {
		my $aid		= $row{ 'agency_id' };
		my $aurl	= $self->{agency}{$aid};
		print qq[\t:agency <$aurl> ;];
	}
	if (my $url = $row{ 'route_url' }) {
		print qq[\tfoaf:homepage <$url> ;];
	}

	print qq[\t.\n\n];
}

################################################################################

sub emit_frequencies {
	my $self	= shift;
	my %row		= @_;
	foreach (qw(trip_id start_time end_time headway_secs)) {
		unless (exists $row{$_}) {
			throw Error -text => "Missing frequencies field $_";
		}
	}
	
	my $tid		= $row{ 'trip_id' };
	my $start	= $row{ 'start_time' };
	my $end		= $row{ 'end_time' };
	my $secs	= $row{ 'headway_secs' };
	
	$self->{trip_frequencies}{$tid}	= [$start, $end, $secs];
}

################################################################################

sub emit_trips {
	my $self	= shift;
	my %row		= @_;
	foreach (qw(route_id service_id trip_id)) {
		unless (exists $row{$_}) {
			throw Error -text => "Missing trips field $_";
		}
	}
	
	my $rid		= $row{ 'route_id' };
	my $sid		= $row{ 'service_id' };
	my $tid		= $row{ 'trip_id' };
	my $headsign	= $row{'trip_headsign'};
	
	my $ruri	= $self->{routes}{$rid} or throw Error -text => "Unknown route id $rid";
	push(@{ $self->{trip_route}{$tid} }, $rid);
	
	my $tripnum	= ($tid =~ /\D/) ? $tid : "#$tid";
	my $desc	= "Route $rid, $sid service, $tripnum";
	
	my $uri		= sprintf('%s/service/%s/%s', $ruri, uri_escape($sid), uri_escape($tid));
	$self->{trips}{$tid}	= $uri;
	$self->{trip_titles}{$tid}	= $desc;
	$self->{route_trips}{$rid}{$tid}	= $uri;
	
	print <<"END";
<$uri> a :Trip ;
	rdfs:label "$desc" ;
	:route <$ruri> ;
	:route_id "$rid" ;
	:service_id "$sid" ;
	:trip_id "$tid" ;
END

	if (my $data = $self->{trip_frequencies}{$tid}) {
		my ($start, $end, $secs)	= @$data;
		print <<"END";
	:start_time "$start" ;
	:end_time "$end" ;
	:headway_seconds "$secs" ;
END
	}
	
	if ($headsign) {
		print qq[\t:trip_headsign "$headsign" ;\n];
	}
	print qq[\t.\n\n];
}

################################################################################

sub emit_stops {
	my $self	= shift;
	my %row		= @_;
#	warn Dumper(\%row);
	foreach (qw(stop_id stop_name stop_lat stop_lon)) {
		unless (exists $row{$_}) {
			throw Error -text => "Missing stops field $_";
		}
	}
	
	my $sid		= $row{ 'stop_id' };
	my $name	= $row{ 'stop_name' };
	my $lat		= $row{ 'stop_lat' };
	my $lon		= $row{ 'stop_lon' };
	my $id		= $self->_make_id( stop_name => $name );
	
	my $uri		= sprintf('%s/stop/%s', $self->base, uri_escape($id));
	$self->{stops}{$sid}	= $uri;
	$self->{stop_titles}{$sid}	= $name;
	
	my $type	= ($row{'location_type'}) ? 'Station' : 'Stop';
	print <<"END";
<$uri> a :${type} ;
	dcterms:identifier "$sid" ;
	rdfs:label "$name" ;
	geo:lat $lat ;
	geo:long $lon ;
END
	if (my $url = $row{ 'stop_url' }) {
		print qq[\tfoaf:homepage <$url> ;];
	}
	
	print qq[\t.\n\n];
}

################################################################################

sub emit_stop_times {
	my $self	= shift;
	my %row		= @_;
	foreach (qw(trip_id arrival_time departure_time stop_id stop_sequence)) {
		unless (exists $row{$_}) {
			throw Error -text => "Missing stop_times field $_";
		}
	}
	
	my $tid		= $row{ 'trip_id' };
	my $arr		= $row{ 'arrival_time' };
	my $dep		= $row{ 'departure_time' };
	my $sid		= $row{ 'stop_id' };
	my $seq		= $row{ 'stop_sequence' };
	
	my $suri	= $self->{stops}{$sid};
	my $turi	= $self->{trips}{$tid};
	my $uri		= sprintf('%s/stop/%s', $turi, uri_escape($seq));
	
	my $stop_title	= $self->{stop_titles}{$sid};
	my $trip_title	= $self->{trip_titles}{$tid};
	my @rids	= @{ $self->{trip_route}{$tid} };
	foreach my $rid (@rids) {
		my $rurl	= $self->{routes}{$rid};
		$self->{stop_routes}{$sid}{$rid}	= $rurl;
	}
	
	$self->{trip_stoptimes}{$tid}{$seq}	= $uri;
	
	print <<"END";
<$uri> a :StopTime ;
	rdfs:label "$stop_title, $trip_title, dep $dep" ;
	:trip <$turi> ;
	:stop <$suri> ;
	:stop_sequence $seq ;
END
	
	unless ($self->{trip_frequencies}{$tid}) {
		print <<"END";
	:arrival_time "$arr" ;
	:departure_time "$dep" ;
END
	}
	print qq[\t;\n\n];
}

################################################################################

sub finish_assertions {
	my $self	= shift;
	# Stop :has_route Route
	foreach my $sid (keys %{ $self->{stops} }) {
		my $surl	= $self->{stops}{$sid};
		foreach my $rid (keys %{ $self->{stop_routes}{$sid} }) {
			my $rurl	= $self->{stop_routes}{$sid}{$rid};
			print qq[<$surl> :has_route <$rurl> .\n];
			print qq[<$rurl> :has_stop <$surl> .\n];
		}
	}
	
	# Route has_trip Trip
	foreach my $rid (keys %{ $self->{routes} }) {
		my $rurl	= $self->{routes}{$rid};
		foreach my $tid (keys %{ $self->{route_trips}{$rid} }) {
			my $turl	= $self->{route_trips}{$rid}{$tid};
			print qq[<$rurl> :has_trip <$turl> .\n];
		}
	}
	
	# Trip has_stoptimes [ StopTime, StopTime, ... ]
	foreach my $tid (keys %{ $self->{trip_stoptimes} }) {
		my $turl	= $self->{trips}{$tid};
		my $title	= $self->{trip_titles}{$tid};
		my $timesurl	= sprintf( '%s/times', $turl );
		print qq[<$turl> :has_stoptimes <$timesurl> .\n];
		print qq[<$timesurl> rdfs:label "Trip times for $title" .\n];
		my $count	= 1;
		my $last;
		foreach my $sid (sort { $a <=> $b } (keys %{ $self->{trip_stoptimes}{$tid} })) {
			my $id	= $count++;
			my $surl	= $self->{trip_stoptimes}{$tid}{$sid};
			print qq[<$timesurl> rdf:_${id} <$surl> .\n];
			if (defined($last)) {
				print qq[];
			}
			$last	= $surl;
		}
	}
	print "\n";
}

sub emit_dataset {
	my $self	= shift;
	print "# dataset\n";
	my $uri		= $self->dataset_uri;
	print <<"END";
<$uri> a void:Dataset ;
	dcterms:subject <http://dbpedia.org/resource/Transport> ;
	void:vocabulary <http://myrdf.us/gtfs/vocab/> ;
END
	if (my $uri = $self->license) {
		print qq[\tdcterms:license <$uri> ;\n];
	}
	if (my $uri = $self->source) {
		print qq[\tdcterms:source <$uri> ;\n];
	}
	
	if (reftype($self->{trip_stoptimes}) eq 'HASH') {
		my ($rid)	= keys %{ $self->{trip_stoptimes} };
		my $stops	= $self->{trip_stoptimes}{ $rid };
		if (reftype($stops) eq 'HASH') {
			my ($sid)	= keys %{ $stops };
			my $ex		= $self->{trip_stoptimes}{ $rid }{ $sid };
			print qq[\tvoid:exampleResource <$ex> ;\n];
		}
	}
	
	print qq[\t.\n\n];
}

################################################################################

sub dataset_uri {
	my $self	= shift;
	return sprintf('%s/dataset', $self->base);
}

sub _bool {
	my $value	= shift;
	return ($value) ? 'true' : 'false';
}

sub _date {
	my $value	= shift;
	if ($value =~ m/^(\d\d\d\d)(\d\d)(\d\d)$/) {
		return qq["$1-$2-$3"^^xsd:date];
	} else {
		throw Error -text => "Not a valid date value: '$value'";
	}
}

sub _make_id {
	my $self	= shift;
	my $field	= shift;
	my $name	= lc(shift);
	$name		=~ s/\s+/_/g;
	return $name;
}

