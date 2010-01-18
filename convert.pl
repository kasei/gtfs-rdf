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
my $split	= 0;
my $output	= 'turtle';
my $result	= GetOptions(
				"base=s"	=> \$base,
				"license=s"	=> \$lic,
				"source=s"	=> \$source,
				"output=s"	=> \$output,
				"split_size=i"	=> \$split,
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
$args{split_size}	= $split if ($split);
$args{output}	= $output;

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

use RDF::Trine;
use RDF::Trine::Node qw(ntriples_escape);

our %ROUTE_TYPES;
BEGIN {
	MTA->mk_accessors(qw(base license source optional_files output split_size));
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

sub namespaces {
	my $self	= shift;
	my $base	= $self->base;
	return sprintf(<<'END', $base);
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dc: <http://purl.org/dc/elements/1.1/> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix void: <http://rdfs.org/ns/void#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix gtfs: <http://myrdf.us/gtfs/vocab/> .

END
END
}

sub init {
	my $self	= shift;
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
	
	my $name	= ntriples_escape( $row{ 'agency_name' } );
	my $url		= $row{ 'agency_url' };
	my $tz		= ntriples_escape( $row{ 'agency_timezone' } );
	my $id		= $self->_make_id( agency_name => $row{ 'agency_name' } );
	
	my $uri		= sprintf('%s/agency/%s', $self->base, uri_escape($id));
	
	if (exists $row{ 'agency_id' }) {
		my $aid	= $row{ 'agency_id' };
		$self->{agency}{$aid}	= $uri;
	}
	
	my $string	= <<"END";
<$uri> a gtfs:Agency ;
	dc:title "$name" ;
	foaf:homepage <$url> ;
	gtfs:timezone "$tz" .

END
	$self->emit_turtle( $string );
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
	my $string	= <<"END";
<$uri> a gtfs:Service ;
	gtfs:start_date $start ;
	gtfs:end_date $end ;
	gtfs:monday $mon ;
	gtfs:tuesday $tue ;
	gtfs:wednesday $wed ;
	gtfs:thursday $thu ;
	gtfs:friday $fri ;
	gtfs:saturday $sat ;
	gtfs:sunday $sun ;
	.

END
	$self->emit_turtle( $string );
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
	my $rids	= ntriples_escape( $rid );
	my $short	= ntriples_escape( $row{ 'route_short_name' } );
	my $long	= ntriples_escape( $row{ 'route_long_name' } );
	my $type	= 0+$row{ 'route_type' };
	my $typeQName	= $ROUTE_TYPES{ $type };
	unless ($typeQName) {
		throw Error -text => "Unknown route type '$type'";
	}
	
	my $name	= $short || $long;
	my $id		= $self->_make_id( route_name => join('-',$rid,$name) );
	
	my $uri		= sprintf('%s/route/%s', $self->base, uri_escape($id));
	$self->{routes}{$rid}	= $uri;
	
	my $string	= <<"END";
<$uri> a gtfs:Route ;
	dcterms:identifier "$rids" ;
	gtfs:route_type gtfs:$typeQName ;
	rdfs:label "$name" ;
END
	$string	.= qq[\tdc:title "$short" ;\n] if ($short);
	$string	.= qq[\tdc:description "$long" ;\n] if ($long);
	
	if (exists $row{ 'agency_id' }) {
		my $aid		= $row{ 'agency_id' };
		my $aurl	= $self->{agency}{$aid};
		$string	.= qq[\tgtfs:agency <$aurl> ;];
	}
	if (my $url = $row{ 'route_url' }) {
		$string	.= qq[\tfoaf:homepage <$url> ;];
	}

	$string	.= qq[\t.\n\n];
	$self->emit_turtle( $string );
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
	
	my $rid			= $row{ 'route_id' };
	my $rids		= ntriples_escape( $rid );
	my $sid			= $row{ 'service_id' };
	my $sids		= ntriples_escape( $sid );
	my $tid			= $row{ 'trip_id' };
	my $tids		= ntriples_escape( $tid );
	my $headsign	= ntriples_escape( $row{'trip_headsign'} );
	
	my $ruri	= $self->{routes}{$rid} or throw Error -text => "Unknown route id $rid";
	push(@{ $self->{trip_route}{$tid} }, $rid);
	
	my $tripnum	= ($tid =~ /\D/) ? $tid : "#$tid";
	my $desc	= ntriples_escape( "Route $rid, $sid service, $tripnum" );
	
	my $uri		= sprintf('%s/service/%s/%s', $ruri, uri_escape($sid), uri_escape($tid));
	$self->{trips}{$tid}	= $uri;
	$self->{trip_titles}{$tid}	= $desc;
	
	my $string	= <<"END";
<$ruri> gtfs:has_trip <$uri> .
<$uri> a gtfs:Trip ;
	rdfs:label "$desc" ;
	gtfs:route <$ruri> ;
	gtfs:route_id "$rids" ;
	gtfs:service_id "$sids" ;
	gtfs:trip_id "$tids" ;
END

	if (my $data = $self->{trip_frequencies}{$tid}) {
		my ($start, $end, $secs)	= map { ntriples_escape($_) } @$data;
		$string	.= <<"END";
	gtfs:start_time "$start" ;
	gtfs:end_time "$end" ;
	gtfs:headway_seconds "$secs" ;
END
	}
	
	if ($headsign) {
		$string	.= qq[\tgtfs:trip_headsign "$headsign" ;\n];
	}
	$string	.= qq[\t.\n\n];
	$self->emit_turtle( $string );
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
	my $sids	= ntriples_escape( $sid );
	my $name	= ntriples_escape( $row{ 'stop_name' } );
	my $lat		= $row{ 'stop_lat' };
	my $lon		= $row{ 'stop_lon' };
	my $id		= $self->_make_id( stop_name => $row{ 'stop_name' } );
	
	my $uri		= sprintf('%s/stop/%s', $self->base, uri_escape($id));
	$self->{stops}{$sid}	= $uri;
	$self->{stop_titles}{$sid}	= $name;
	
	my $type	= ($row{'location_type'}) ? 'Station' : 'Stop';
	my $string	= <<"END";
<$uri> a gtfs:${type} ;
	dcterms:identifier "$sids" ;
	rdfs:label "$name" ;
	geo:lat $lat ;
	geo:long $lon ;
END
	if (my $url = $row{ 'stop_url' }) {
		$string	.= qq[\tfoaf:homepage <$url> ;];
	}
	
	$string	.= qq[\t.\n\n];
	$self->emit_turtle( $string );
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
	my $tids	= ntriples_escape( $tid );
	my $arr		= ntriples_escape( $row{ 'arrival_time' } );
	my $dep		= ntriples_escape( $row{ 'departure_time' } );
	my $sid		= $row{ 'stop_id' };
	my $sids	= ntriples_escape( $sid );
	my $seq		= $row{ 'stop_sequence' };
	
	my $suri	= $self->{stops}{$sid};
	my $turi	= $self->{trips}{$tid};
	my $uri		= sprintf('%s/stop/%s', $turi, uri_escape($seq));
	
	my $stop_title	= $self->{stop_titles}{$sid};
	my $trip_title	= $self->{trip_titles}{$tid};
	my @rids	= @{ $self->{trip_route}{$tid} };
	foreach my $rid (@rids) {
		my $ruri	= $self->{routes}{$rid};
		$self->emit_turtle( <<"END" );
<$suri> gtfs:has_route <$ruri> .
<$ruri> gtfs:has_stop <$suri> .
END
	}
	
	$self->{trip_stoptimes}{$tid}{$seq}	= $uri;
	
	my $string	= <<"END";
<$uri> a gtfs:StopTime ;
	rdfs:label "$stop_title, $trip_title, dep $dep" ;
	gtfs:trip <$turi> ;
	gtfs:stop <$suri> ;
	gtfs:stop_sequence $seq ;
END
	
	unless ($self->{trip_frequencies}{$tid}) {
		$string	.= <<"END";
	gtfs:arrival_time "$arr" ;
	gtfs:departure_time "$dep" ;
END
	}
	$string	.= qq[\t.\n\n];
	$self->emit_turtle( $string );
}

################################################################################

sub finish_assertions {
	my $self	= shift;
	
	# Trip has_stoptimes [ StopTime, StopTime, ... ]
	print "# back filling trips to stoptimes\n";
	foreach my $tid (keys %{ $self->{trip_stoptimes} }) {
		my $turl	= $self->{trips}{$tid};
		my $title	= $self->{trip_titles}{$tid};
		my $timesurl	= sprintf( '%s/times', $turl );
		print qq[<$turl> gtfs:has_stoptimes <$timesurl> .\n];
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

sub emit_turtle {
	my $self	= shift;
	my $turtle	= shift;
	
	unless (exists($self->{split_records})) {
		$self->{split_records}	= 0;
	}
	
	my $o		= $self->output;
	if (my $size = $self->split_size) {
		if ($self->{split_records} >= $size) {
			print "----------\n";
			$self->{split_records}	= 0;
		}
	}
	
	unless ($self->{split_records}) {
		if ($o eq 'turtle') {
			print $self->namespaces;
		}
	}
	
	$self->{split_records}++;
	if ($o eq 'turtle') {
		print $turtle;
	} else {
		my $model	= RDF::Trine::Model->temporary_model;
		my $parser	= RDF::Trine::Parser->new( 'turtle' );
		my $rdf		= $self->namespaces . $turtle;
		$parser->parse_into_model( $self->base . '/', $rdf, $model );
		my $serializer = RDF::Trine::Serializer::NTriples->new();
		$serializer->serialize_model_to_file( \*STDOUT, $model );
	}
}

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

