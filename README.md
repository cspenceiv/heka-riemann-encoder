# heka-riemann-encoder

This is an encoder plugin for [heka](https://github.com/mozilla-services/heka) that allows heka to forward events to [Riemann](http://riemann.io). This allows users to leverage the existing TCP output plugin (and TLS support) provided by heka.

## Credits

The general gist of riemann communication was based off of [raidman](https://github.com/amir/raidman).
