# Timeweb Zabbix agent firewall

Host: `msk-1-vm-3mxl`. October 22 audit found general IPv4 and IPv6 UFW
allow rules for 10050/TCP, while `/etc/zabbix/zabbix_agentd.conf` listed
`92.53.116.12,92.53.116.111,92.53.116.119` as allowed servers.
No agent reinstall, SSH change, firewall reset or service restart is needed.
The user applied this procedure on October 22. Reported UFW output confirms
only the three provider IPv4 addresses can access 10050/TCP; both generic
IPv4/IPv6 agent rules were removed, and both OpenSSH rules remain unchanged.
Fresh Timeweb polling samples and a second SSH login still need verification.
The commands below are the reproducible procedure, not a request to rerun it.

## Before changing rules

Keep the current SSH session open and retain Timeweb console access. Check:

```bash
ufw status numbered
grep -E '^(Server|ServerActive|ListenIP|ListenPort)=' /etc/zabbix/zabbix_agentd.conf
ss -ltnp '( sport = :10050 )'
```

Proceed only if UFW is active, OpenSSH remains allowed, and the agent server
list still matches those three IPv4 addresses. If the configuration differs,
stop and reconcile it with the provider before changing the allowlist. Do not
guess additional IPv6 addresses or replace the agent's Server configuration.

## Narrow only the agent rule

First add the three specific rules. If any command fails, stop before deletion.

```bash
ufw allow from 92.53.116.12 to any port 10050 proto tcp
ufw allow from 92.53.116.111 to any port 10050 proto tcp
ufw allow from 92.53.116.119 to any port 10050 proto tcp
ufw status numbered
```

Confirm all three specific rules are present. Then delete only the generic
port rule, by its specification rather than unstable numbered positions:

```bash
ufw delete allow 10050/tcp
ufw status numbered
```

Expected: OpenSSH rules unchanged; three IPv4 rules for 10050/TCP; no generic
`Anywhere` or `Anywhere (v6)` rule for that port. UFW applies these changes
immediately; do not run `ufw reset`, change default policies or restart networking.
If generic IPv6 access remains, inspect the exact rule before removing it.

## Verify and rollback

Open a second SSH session before closing the first. Confirm Timeweb's graph
keeps receiving new samples after its normal polling interval; the historical
graph alone is not proof of new polling. No restart of the HH collector is needed.
If samples stop, check source IPs/configuration before expanding permissions.
Temporary rollback of this specific port change, if necessary:

```bash
ufw allow 10050/tcp
```

This rollback restores broad agent access, not a desirable final state.
Do not rerun `wget http://... | bash`: the package is already installed and
that installer opens the broad rule again.
