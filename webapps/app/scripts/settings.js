window.settings = {};
settings.twitter = {};
settings.mobile = {};
settings.mobile.topic = {};
settings.machine = {};
settings.fraud = {};

settings.webSocketUrl = 'ws://localhost:3390/pubsub';
settings.appsURL = 'http://localhost:3390/static/#ops/apps/';

settings.twitter.appName = 'TwitterDemoApplication';
settings.twitter.topic = 'demos.twitter.topURLs';
settings.mobile.topic.out = 'demos.mobile.phoneLocationQueryResult';
settings.mobile.topic.in = 'demos.mobile.phoneLocationQuery';
settings.mobile.appName = 'MobileDemoApplication';
settings.machine.appName = 'MCScalableApplication';
settings.machine.metricformat = '#.0';
settings.fraud.appName = 'Fraud-Detection-Demo-Application';
