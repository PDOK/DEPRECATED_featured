truncate featured.feature;
truncate featured.feature_stream;
truncate featured.timeline;
truncate featured.timeline_delta;
truncate featured.timeline_current;
truncate featured.timeline_current_delta;

truncate extractmanagement.d1_integration_v0 cascade;
drop schema if exists d1 cascade;