# appx_gpstracker (WIP)
Generic LoRa application to consume TrackNet LNS events

## Features
* Network outages/disconnects tolerance
* Support white list filters by DevEui and MsgType
* Support regexp in white list filters
* Support filters hot reloading (SIGHUP)
* Instrumented with Prometheus
* Both ws and secured wss supported
* Dynamic TCIO autoconfiguration support

## ToDo's
* Track last FCntUp/Down and restart fetching from last state

## Config example
```yaml
appname: gpstracker

owner:
  id: "owner-1::"
  appx_bootstrap_uri: ws://lns.xxx:7000/owner-info
  storage_pref_list: [mongo]

  ssl:
  certificate: /etc/var/foo.crt
  public_key: /etc/var/foo.key
  trust_chain: /etc/var/trust_foo.crt

mongo:
  uri: "mongo://..."

filters:
  deveui:
    - ".*"
    #- 00-01-00-00-00-00-00-00
    #- 00-01-00-00-00-00-00-01
    #- 00-01-00-00-00-00-00-02
  msg_type: 
    - "*"
```