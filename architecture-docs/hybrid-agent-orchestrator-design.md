# Hybrid Orchestrator Implementation Guide

## Overview

The hybrid orchestrator combines the best of both worlds:

- **Autonomous agents** (Inventory, Migration, Reporting) operate independently and react to events in real-time
- **Centralized orchestrators** (Compliance, Physical) enforce strict governance over critical operations
- **Message bus** (Kafka/RabbitMQ) coordinates event-driven agents without creating tight coupling

This pattern gives you:
- **Parallelism**: Multiple migrations can happen at once
- **Speed**: No central orchestrator bottleneck
- **Governance**: Compliance and physical operations stay under control
- **Resilience**: Agent failures are isolated

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Message Bus (Kafka)                        │
│  Events: MIGRATION_APPROVED, MIGRATION_COMPLETED, etc.         │
└──────────┬──────────────────────────────────────────────────────┘
           │
    ┌──────┼──────┬──────────┬─────────────┐
    │      │      │          │             │
    │ [Autonomous Agents]    │  [Centralized]
    │      │      │          │
    ↓      ↓      ↓          ↓             ↓
┌─────┐┌──────┐┌────────┐┌────────┐┌──────┐
│Inv. ││Mig.  ││Report. ││Compl.  ││Phys. │
│Agent││Agent ││Agent   ││Orches. ││Orches│
└─────┘└──────┘└────────┘└────────┘└──────┘

     Subscription Model:
     
     Inventory Agent:
       → SUBSCRIBE: REFRESH_INVENTORY (event), SCHEDULE:6h (periodic)
       → PUBLISH: INVENTORY_UPDATED, BLOCKERS_DETECTED
     
     Migration Agent:
       → SUBSCRIBE: MIGRATION_APPROVED, INVENTORY_UPDATED
       → PUBLISH: MIGRATION_STARTED, MIGRATION_COMPLETED, MIGRATION_FAILED
     
     Compliance Orchestrator (Central):
       → SUBSCRIBE: MIGRATION_COMPLETED
       → PUBLISH: COMPLIANCE_CLEARED, COMPLIANCE_BLOCKED (via API call to Compliance Agent)
       → Makes approvals, governs erase operations
     
     Physical Orchestrator (Central):
       → SUBSCRIBE: ALL_DATA_ERASED
       → PUBLISH: POWERDOWN_APPROVED (via API call to Physical Agent)
       → Makes approvals, governs power-down operations
     
     Reporting Agent:
       → SUBSCRIBE: TASK_COMPLETED, ESCALATION_CREATED
       → PUBLISH: REPORT_GENERATED
```

---

## Design Philosophy

**Autonomous agents** handle:
- Observation (fast, no governance needed)
- Migration (parallelizable, approval-gated)
- Reporting (informational, no side effects)

**Centralized orchestrators** handle:
- Compliance (must be auditable, requires approvals from compliance officer)
- Physical operations (must be coordinated, has long lead times)

**Why this split?**
- Migrations are fast (30 min – 2h) and parallelizable → async/autonomous
- Compliance checks take minutes → can be async via message bus
- Physical operations are slow (days) and require human coordination → centralized

---

## Autonomous Agents Pattern

Autonomous agents subscribe to events and react to them independently. No central orchestrator bottleneck.

### 1.1 Inventory Agent (Autonomous)

```python
import asyncio
from kafka import KafkaConsumer, KafkaProducer
import json

class InventoryAgent:
    """
    Autonomous inventory agent.
    
    Listens for:
      - REFRESH_INVENTORY (on-demand refresh)
      - SCHEDULE:6h (periodic refresh)
    
    Publishes:
      - INVENTORY_UPDATED
      - DEPENDENCY_CHANGED
      - NEW_BLOCKER_DETECTED
    """
    
    def __init__(self, config):
        self.config = config
        self.state_db = PostgresStateDB(config['db'])
        self.audit_log = AuditLog(self.state_db)
        
        # Message bus
        self.consumer = KafkaConsumer(
            bootstrap_servers=config['kafka_brokers'],
            group_id='inventory-agent',
            auto_offset_reset='earliest'
        )
        self.producer = KafkaProducer(
            bootstrap_servers=config['kafka_brokers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # System connectors
        self.storage = StorageArrayConnector(config['storage'])
        self.vmware = VMwareConnector(config['vmware'])
        self.backup = BackupConnector(config['backup'])
        self.dr = DRConnector(config['dr'])
        
        self.logger = Logger("InventoryAgent")
    
    async def run(self):
        """Main event loop."""
        
        # Subscribe to relevant topics
        self.consumer.subscribe([
            'REFRESH_INVENTORY',      # On-demand
            'SCHEDULE:6h',            # Periodic
            'SCHEDULE:startup'        # On startup
        ])
        
        while True:
            try:
                # Poll for messages (non-blocking)
                messages = self.consumer.poll(timeout_ms=1000, max_records=1)
                
                for topic_partition, messages_list in messages.items():
                    for message in messages_list:
                        topic = topic_partition.topic
                        
                        if topic in ['REFRESH_INVENTORY', 'SCHEDULE:6h', 'SCHEDULE:startup']:
                            # Trigger a full inventory scan
                            await self.full_scan()
                        
                        self.consumer.commit()
                
                await asyncio.sleep(0.1)
            
            except Exception as e:
                self.logger.error(f"Inventory agent error: {e}", exc_info=True)
                await asyncio.sleep(10)
    
    async def full_scan(self):
        """Scan all systems and update inventory."""
        
        scan_start = now()
        self.logger.info("Starting full inventory scan...")
        
        try:
            # Fetch current state from all systems
            storage_luns = await self.storage.list_luns()
            vmware_datastores = await self.vmware.list_datastores()
            backup_jobs = await self.backup.list_backup_jobs()
            dr_pairs = await self.dr.list_replication_pairs()
            
            self.logger.info(
                f"Scan results: {len(storage_luns)} LUNs, "
                f"{len(vmware_datastores)} datastores, "
                f"{len(backup_jobs)} backup jobs"
            )
            
            # Process inventory
            updates = self.process_inventory(
                storage_luns,
                vmware_datastores,
                backup_jobs,
                dr_pairs
            )
            
            # Publish results
            self.publish_inventory_update(updates)
            
            scan_duration = (now() - scan_start).total_seconds()
            self.logger.info(f"Full scan completed in {scan_duration:.1f}s")
            
            self.audit_log.record_action(
                timestamp=now(),
                agent='inventory',
                action='FULL_SCAN',
                result=f"Updated {updates['new_assets']} assets, "
                       f"{updates['changed_assets']} changes, "
                       f"removed {updates['removed_assets']}"
            )
        
        except Exception as e:
            self.logger.error(f"Full scan failed: {e}", exc_info=True)
            # Publish error event (other agents will see it and may escalate)
            self.producer.send('INVENTORY_SCAN_FAILED', {
                'error': str(e),
                'timestamp': now().isoformat()
            })
    
    def process_inventory(self, storage_luns, vmware_datastores, backup_jobs, dr_pairs):
        """
        Compare current state against DB.
        Detect new, changed, and removed assets.
        """
        
        updates = {
            'new_assets': 0,
            'changed_assets': 0,
            'removed_assets': 0,
            'new_dependencies': 0
        }
        
        # ===== PROCESS STORAGE LUNS =====
        for lun in storage_luns:
            existing = self.state_db.get_asset(external_id=lun.external_id)
            
            if not existing:
                # New LUN
                asset = self.state_db.create_asset(
                    type='LUN',
                    external_id=lun.external_id,
                    name=lun.name,
                    size_gb=lun.size_gb,
                    status='ACTIVE',
                    metadata={
                        'array_id': lun.array_id,
                        'serial_number': lun.serial_number,
                        'discovered_at': now().isoformat()
                    }
                )
                self.logger.info(f"New LUN discovered: {lun.name}")
                updates['new_assets'] += 1
            
            elif existing['size_gb'] != lun.size_gb or existing['name'] != lun.name:
                # Changed LUN
                self.state_db.update_asset(
                    existing['id'],
                    size_gb=lun.size_gb,
                    name=lun.name
                )
                self.logger.warn(f"LUN changed: {lun.name}")
                updates['changed_assets'] += 1
        
        # ===== PROCESS VMWARE DATASTORES & VMS =====
        for datastore in vmware_datastores:
            # Find backing LUN
            backing_lun = next(
                (l for l in storage_luns if l.external_id == datastore.lun_id),
                None
            )
            
            if backing_lun:
                source_asset = self.state_db.get_asset(external_id=backing_lun.external_id)
                
                # Create/find datastore asset
                ds_asset = self.state_db.get_or_create_asset(
                    type='DATASTORE',
                    external_id=datastore.external_id,
                    name=datastore.name
                )
                
                # Create dependency: LUN → Datastore
                self.state_db.create_dependency(
                    source_id=source_asset['id'],
                    target_id=ds_asset['id'],
                    relationship='MOUNTED_ON'
                )
                
                # Process VMs on this datastore
                for vm in datastore.vms:
                    vm_asset = self.state_db.get_or_create_asset(
                        type='VM',
                        external_id=vm.external_id,
                        name=vm.name,
                        metadata={
                            'owner': vm.owner_contact,
                            'team': vm.team
                        }
                    )
                    
                    # Create dependency: Datastore → VM
                    self.state_db.create_dependency(
                        source_id=ds_asset['id'],
                        target_id=vm_asset['id'],
                        relationship='HOSTS'
                    )
                    
                    updates['new_dependencies'] += 1
        
        # ===== PROCESS BACKUP JOBS =====
        for job in backup_jobs:
            source_asset = self.state_db.get_asset(external_id=job.source_lun_id)
            
            if source_asset:
                # Link backup job to LUN
                self.state_db.update_asset(
                    source_asset['id'],
                    metadata={
                        **source_asset.get('metadata', {}),
                        'backup_job_id': job.id,
                        'backup_system': job.system,
                        'last_backup': job.last_run_time.isoformat(),
                        'backup_age_hours': (now() - job.last_run_time).total_seconds() / 3600
                    }
                )
        
        # ===== PROCESS DR PAIRS =====
        for pair in dr_pairs:
            source = self.state_db.get_asset(external_id=pair.source_lun_id)
            target = self.state_db.get_asset(external_id=pair.target_lun_id)
            
            if source and target:
                self.state_db.create_dependency(
                    source_id=source['id'],
                    target_id=target['id'],
                    relationship='REPLICATED_TO'
                )
        
        return updates
    
    def publish_inventory_update(self, updates):
        """Publish inventory update event."""
        
        event = {
            'timestamp': now().isoformat(),
            'agent': 'inventory',
            'event_type': 'INVENTORY_UPDATED',
            'updates': updates
        }
        
        self.producer.send('INVENTORY_UPDATED', event)
        self.logger.info(f"Published INVENTORY_UPDATED event: {updates}")
```

### 1.2 Migration Agent (Autonomous)

```python
class MigrationAgent:
    """
    Autonomous migration agent.
    
    Listens for:
      - MIGRATION_APPROVED (PM or orchestrator approved a migration)
      - INVENTORY_UPDATED (to re-compute safe migration order)
    
    Publishes:
      - MIGRATION_STARTED
      - MIGRATION_COMPLETED
      - MIGRATION_FAILED
    """
    
    def __init__(self, config):
        self.config = config
        self.state_db = PostgresStateDB(config['db'])
        self.audit_log = AuditLog(self.state_db)
        
        self.consumer = KafkaConsumer(
            bootstrap_servers=config['kafka_brokers'],
            group_id='migration-agent',
            auto_offset_reset='earliest'
        )
        self.producer = KafkaProducer(
            bootstrap_servers=config['kafka_brokers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.storage = StorageArrayConnector(config['storage'])
        self.vmware = VMwareConnector(config['vmware'])
        self.logger = Logger("MigrationAgent")
    
    async def run(self):
        """Main event loop."""
        
        self.consumer.subscribe([
            'MIGRATION_APPROVED',
            'INVENTORY_UPDATED'
        ])
        
        # Concurrency limiter: max 5 concurrent migrations
        self.semaphore = asyncio.Semaphore(5)
        
        # Track active migrations
        self.active_migrations = {}
        
        while True:
            try:
                messages = self.consumer.poll(timeout_ms=1000, max_records=10)
                
                for topic_partition, messages_list in messages.items():
                    for message in messages_list:
                        topic = topic_partition.topic
                        event = json.loads(message.value)
                        
                        if topic == 'MIGRATION_APPROVED':
                            # Trigger migration (respects semaphore)
                            asyncio.create_task(
                                self.execute_migration_with_semaphore(event)
                            )
                        
                        elif topic == 'INVENTORY_UPDATED':
                            # Re-compute migration order
                            await self.recompute_migration_order()
                        
                        self.consumer.commit()
                
                await asyncio.sleep(0.1)
            
            except Exception as e:
                self.logger.error(f"Migration agent error: {e}", exc_info=True)
                await asyncio.sleep(10)
    
    async def execute_migration_with_semaphore(self, event):
        """
        Execute migration with concurrency limit.
        
        At most 5 migrations run in parallel.
        """
        
        async with self.semaphore:
            asset_id = event['asset_id']
            asset = self.state_db.get_asset(asset_id)
            
            self.logger.info(f"Starting migration: {asset['name']}")
            self.active_migrations[asset_id] = now()
            
            try:
                result = await self.execute_migration(asset, event)
                
                # Publish completion event
                self.producer.send('MIGRATION_COMPLETED', {
                    'timestamp': now().isoformat(),
                    'asset_id': asset_id,
                    'status': 'SUCCESS' if result['success'] else 'FAILED',
                    'result': result
                })
                
            except Exception as e:
                self.logger.error(f"Migration failed: {e}", exc_info=True)
                
                self.producer.send('MIGRATION_FAILED', {
                    'timestamp': now().isoformat(),
                    'asset_id': asset_id,
                    'error': str(e)
                })
            
            finally:
                del self.active_migrations[asset_id]
    
    async def execute_migration(self, asset, event):
        """
        Execute the actual migration.
        
        Steps:
        1. Verify dependencies (all sources migrated)
        2. Capture pre-migration baseline
        3. Execute storage migration
        4. Validate post-migration
        5. Update asset status
        """
        
        # Step 1: Check dependencies
        blocking_deps = self.state_db.get_blocking_dependencies(asset['id'])
        if blocking_deps:
            raise Exception(
                f"Cannot migrate: {len(blocking_deps)} dependencies not migrated"
            )
        
        # Step 2: Capture baseline
        baseline = await self.capture_baseline(asset)
        
        # Step 3: Execute migration
        self.logger.info(f"Executing migration: {asset['name']} → {event['target_array']}")
        
        migration_result = await self.storage.migrate_lun(
            source_lun_id=asset['external_id'],
            target_array=event['target_array']
        )
        
        if not migration_result.success:
            raise Exception(f"Migration failed: {migration_result.error}")
        
        # Step 4: Validate post-migration
        validation = await self.validate_migration(asset, baseline)
        
        if not validation['success']:
            self.logger.error(f"Post-migration validation failed: {validation['error']}")
            raise Exception(f"Validation failed: {validation['error']}")
        
        # Step 5: Update asset status
        self.state_db.update_asset(
            asset['id'],
            status='MIGRATED',
            metadata={
                **asset.get('metadata', {}),
                'migrated_at': now().isoformat(),
                'target_array': event['target_array'],
                'migration_duration_seconds': migration_result.duration_seconds,
                'baseline': baseline,
                'post_migration': validation
            }
        )
        
        self.logger.info(f"Migration completed: {asset['name']}")
        
        return {
            'success': True,
            'duration_seconds': migration_result.duration_seconds,
            'baseline': baseline,
            'validation': validation
        }
    
    async def capture_baseline(self, asset):
        """Measure latency, IOPS, throughput before migration."""
        
        return {
            'timestamp': now().isoformat(),
            'latency_ms': await self.vmware.measure_latency(asset['external_id']),
            'iops': await self.storage.measure_iops(asset['external_id']),
            'throughput_gbps': await self.storage.measure_throughput(asset['external_id'])
        }
    
    async def validate_migration(self, asset, baseline):
        """
        Validate post-migration health.
        Check 3 times over 3 minutes.
        """
        
        for i in range(3):
            if i > 0:
                await asyncio.sleep(60)
            
            post = await self.capture_baseline(asset)
            
            # Check for degradation
            latency_degradation = (
                (post['latency_ms'] - baseline['latency_ms']) / baseline['latency_ms']
            )
            
            if abs(latency_degradation) > 0.2:  # >20% change
                self.logger.warn(
                    f"Latency degradation: {latency_degradation:.1%} "
                    f"({baseline['latency_ms']:.1f}ms → {post['latency_ms']:.1f}ms)"
                )
            
            # Check if mounted
            mounted = await self.vmware.check_mount_status(asset['external_id'])
            if not mounted:
                return {
                    'success': False,
                    'error': 'Datastore unmounted after migration',
                    'check_number': i + 1
                }
        
        return {
            'success': True,
            'final_latency_ms': post['latency_ms'],
            'final_iops': post['iops'],
            'final_throughput_gbps': post['throughput_gbps'],
            'checks_passed': 3
        }
    
    async def recompute_migration_order(self):
        """
        When inventory changes, re-compute safe migration order.
        
        This ensures migrations don't violate dependencies.
        """
        
        self.logger.info("Recomputing migration order after inventory change...")
        
        order = self.state_db.compute_migration_order()
        waves = self.state_db.generate_migration_waves(order, max_concurrent=5)
        
        # Update DB with new order (if changed)
        self.state_db.update_migration_waves(waves)
        
        self.logger.info(f"New order: {len(waves)} waves, {sum(len(w) for w in waves)} assets")
```

### 1.3 Reporting Agent (Autonomous)

```python
class ReportingAgent:
    """
    Autonomous reporting agent.
    
    Listens for:
      - TASK_COMPLETED
      - MIGRATION_COMPLETED
      - COMPLIANCE_CLEARED
      - ESCALATION_CREATED
    
    Publishes:
      - REPORT_GENERATED
      - ALERT_ISSUED
    """
    
    def __init__(self, config):
        self.config = config
        self.state_db = PostgresStateDB(config['db'])
        self.audit_log = AuditLog(self.state_db)
        
        self.consumer = KafkaConsumer(
            bootstrap_servers=config['kafka_brokers'],
            group_id='reporting-agent',
            auto_offset_reset='earliest'
        )
        self.producer = KafkaProducer(
            bootstrap_servers=config['kafka_brokers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.logger = Logger("ReportingAgent")
    
    async def run(self):
        """Main event loop."""
        
        self.consumer.subscribe([
            'TASK_COMPLETED',
            'MIGRATION_COMPLETED',
            'COMPLIANCE_CLEARED',
            'ESCALATION_CREATED'
        ])
        
        # Schedule for daily report
        asyncio.create_task(self.schedule_daily_report())
        asyncio.create_task(self.schedule_weekly_report())
        
        while True:
            try:
                messages = self.consumer.poll(timeout_ms=1000, max_records=10)
                
                for topic_partition, messages_list in messages.items():
                    for message in messages_list:
                        event = json.loads(message.value)
                        
                        # Update metrics in real-time
                        await self.update_metrics(event)
                        
                        # Check for anomalies
                        await self.check_anomalies()
                        
                        self.consumer.commit()
                
                await asyncio.sleep(0.1)
            
            except Exception as e:
                self.logger.error(f"Reporting agent error: {e}", exc_info=True)
                await asyncio.sleep(10)
    
    async def update_metrics(self, event):
        """Update burn-down and other metrics based on event."""
        
        # Compute new snapshot
        snapshot = self.compute_burndown_snapshot()
        velocity = self.compute_velocity()
        forecast = self.compute_forecast(snapshot, velocity)
        
        self.state_db.store_burndown_snapshot(snapshot)
        
        self.producer.send('METRICS_UPDATED', {
            'timestamp': now().isoformat(),
            'snapshot': snapshot,
            'velocity': velocity,
            'forecast': forecast
        })
    
    async def check_anomalies(self):
        """Check for anomalies and alert if needed."""
        
        velocity = self.compute_velocity()
        previous_velocity = self.compute_velocity(days=14)
        
        # Alert if velocity dropped 50%
        if velocity < previous_velocity * 0.5:
            self.producer.send('ALERT', {
                'timestamp': now().isoformat(),
                'severity': 'WARN',
                'message': f"Velocity dropped {(1 - velocity/previous_velocity):.0%}. "
                          f"Current: {velocity:.1f} assets/day"
            })
        
        # Alert if many blockers
        blockers = self.state_db.count_blockers()
        if blockers > 5:
            self.producer.send('ALERT', {
                'timestamp': now().isoformat(),
                'severity': 'WARN',
                'message': f"{blockers} blockers active. Check dashboard."
            })
    
    async def schedule_daily_report(self):
        """Generate report every day at 06:00 UTC."""
        
        while True:
            now_time = datetime.now(pytz.UTC)
            target = now_time.replace(hour=6, minute=0, second=0, microsecond=0)
            
            if now_time > target:
                target += timedelta(days=1)
            
            sleep_seconds = (target - now_time).total_seconds()
            await asyncio.sleep(sleep_seconds)
            
            # Generate report
            report = self.generate_daily_report()
            
            self.producer.send('REPORT_GENERATED', {
                'timestamp': now().isoformat(),
                'report_type': 'DAILY',
                'report': report
            })
            
            self.logger.info("Daily report generated")
    
    async def schedule_weekly_report(self):
        """Generate report every Monday at 08:00 UTC."""
        
        while True:
            now_time = datetime.now(pytz.UTC)
            
            # Find next Monday 08:00
            days_until_monday = (7 - now_time.weekday()) % 7
            if days_until_monday == 0 and now_time.hour >= 8:
                days_until_monday = 7
            
            target = now_time + timedelta(days=days_until_monday)
            target = target.replace(hour=8, minute=0, second=0, microsecond=0)
            
            sleep_seconds = (target - now_time).total_seconds()
            await asyncio.sleep(sleep_seconds)
            
            # Generate report
            report = self.generate_weekly_report()
            
            self.producer.send('REPORT_GENERATED', {
                'timestamp': now().isoformat(),
                'report_type': 'WEEKLY',
                'report': report
            })
            
            self.logger.info("Weekly report generated")
    
    def generate_daily_report(self):
        """Generate daily status report."""
        
        snapshot = self.compute_burndown_snapshot()
        velocity = self.compute_velocity()
        
        return {
            'title': f"Decommission Daily Report - {date.today()}",
            'summary': {
                'percent_complete': f"{snapshot['pct_complete']:.0%}",
                'assets_remaining': snapshot['remaining'],
                'velocity': f"{velocity:.1f} assets/day",
                'blockers': self.state_db.count_blockers()
            },
            'top_blockers': self.get_top_blockers(limit=3)
        }
    
    def generate_weekly_report(self):
        """Generate weekly executive report."""
        
        snapshot = self.compute_burndown_snapshot()
        velocity = self.compute_velocity(days=7)
        forecast = self.compute_forecast(snapshot, velocity)
        risk = self.compute_risk_score()
        
        return {
            'title': f"Decommission Weekly Report - Week of {date.today()}",
            'executive_summary': {
                'status': f"{snapshot['pct_complete']:.0%} complete",
                'eta': forecast['eta'].strftime('%Y-%m-%d'),
                'risk_level': risk['level']
            },
            'burndown': snapshot,
            'velocity': {
                'current': f"{velocity:.1f} assets/day",
                'trend': self.compute_velocity_trend()
            },
            'blockers': self.state_db.count_blockers(),
            'escalations': self.state_db.count_escalations(days=7),
            'next_steps': self.generate_recommendations()
        }
```

---

## Centralized Orchestrators

These manage governance-critical operations: compliance and physical tasks.

### 2.1 Compliance Orchestrator (Central)

```python
class ComplianceOrchestrator:
    """
    Centralized orchestrator for compliance operations.
    
    Listens for:
      - MIGRATION_COMPLETED (check compliance before erase)
    
    Publishes:
      - COMPLIANCE_CLEARED (ready to erase)
      - COMPLIANCE_BLOCKED (escalate)
      - ERASE_APPROVED (start erase)
    
    Enforces strict governance:
      - All erasures must be approved by compliance officer
      - Full audit trail
      - Evidence collection
    """
    
    def __init__(self, config):
        self.config = config
        self.state_db = PostgresStateDB(config['db'])
        self.audit_log = AuditLog(self.state_db)
        
        self.consumer = KafkaConsumer(
            bootstrap_servers=config['kafka_brokers'],
            group_id='compliance-orchestrator',
            auto_offset_reset='earliest'
        )
        self.producer = KafkaProducer(
            bootstrap_servers=config['kafka_brokers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.backup = BackupConnector(config['backup'])
        self.storage = StorageArrayConnector(config['storage'])
        self.messaging = MessagingConnector(config['messaging'])
        self.approval_manager = ApprovalManager(self.state_db, self.audit_log)
        
        self.logger = Logger("ComplianceOrchestrator")
    
    async def run(self):
        """Main loop: observe → reason → decide → act → log."""
        
        self.consumer.subscribe(['MIGRATION_COMPLETED'])
        
        while True:
            try:
                messages = self.consumer.poll(timeout_ms=1000, max_records=10)
                
                for topic_partition, messages_list in messages.items():
                    for message in messages_list:
                        event = json.loads(message.value)
                        
                        # Handle migration completion
                        await self.on_migration_completed(event)
                        
                        self.consumer.commit()
                
                # Also check for approval timeouts
                await self.check_approval_timeouts()
                
                await asyncio.sleep(60)
            
            except Exception as e:
                self.logger.error(f"Compliance orchestrator error: {e}", exc_info=True)
                await asyncio.sleep(10)
    
    async def on_migration_completed(self, event):
        """
        Migration completed: check compliance before proceeding to erase.
        """
        
        asset_id = event['asset_id']
        asset = self.state_db.get_asset(asset_id)
        
        self.logger.info(f"Compliance check for {asset['name']}")
        
        # === OBSERVE: Check compliance blockers ===
        blockers = await self.check_compliance_blockers(asset)
        
        # === REASON: Decide next step ===
        if blockers['items']:
            # There are blockers
            self.logger.warn(f"Compliance blockers found for {asset['name']}")
            
            self.state_db.create_task(
                asset_id=asset_id,
                task_type='ESCALATE_COMPLIANCE',
                status='ESCALATED',
                metadata={
                    'blockers': blockers['items'],
                    'reason': blockers['summary']
                }
            )
            
            self.producer.send('COMPLIANCE_BLOCKED', {
                'timestamp': now().isoformat(),
                'asset_id': asset_id,
                'blockers': blockers['items']
            })
            
            self.audit_log.record_decision(
                timestamp=now(),
                action='COMPLIANCE_CHECK',
                asset_id=asset_id,
                decision='BLOCKED',
                reason=blockers['summary']
            )
        
        else:
            # Compliance passed: request erase approval
            self.logger.info(f"Compliance cleared for {asset['name']}")
            
            # === DECIDE: Create erase task and request approval ===
            erase_task = self.state_db.create_task(
                asset_id=asset_id,
                task_type='ERASE',
                status='AWAITING_APPROVAL',
                metadata={
                    'erase_method': self.determine_erase_method(asset),
                    'reason': 'Compliance check passed'
                }
            )
            
            # === ACT: Request approval from compliance officer ===
            approval = await self.approval_manager.request_approval(
                task_id=erase_task['id'],
                approver=self.config['compliance_officer_email'],
                message=self.format_erase_approval_message(asset, erase_task),
                timeout_hours=72
            )
            
            if approval['approved']:
                # Approved: trigger erase
                await self.trigger_erase(asset, erase_task)
            else:
                # Denied or timeout
                erase_task['status'] = 'BLOCKED'
                erase_task['block_reason'] = approval['reason']
                self.state_db.update_task(erase_task)
                
                self.producer.send('ERASE_APPROVAL_DENIED', {
                    'timestamp': now().isoformat(),
                    'asset_id': asset_id,
                    'reason': approval['reason']
                })
            
            # === LOG: Record the decision ===
            self.audit_log.record_decision(
                timestamp=now(),
                action='COMPLIANCE_CHECK',
                asset_id=asset_id,
                decision='CLEARED',
                approver=approval['approver'] if approval['approved'] else None
            )
    
    async def check_compliance_blockers(self, asset):
        """
        Check:
        1. Backup coverage (recent backup exists)
        2. Backup integrity (verification passed)
        3. Retention policies (no holds)
        4. Legal holds
        5. Data classification (requires specific erase method)
        """
        
        blockers = []
        
        # Check 1: Backup coverage
        backup_age_days = asset['metadata'].get('backup_age_hours', 999) / 24
        
        if backup_age_days > self.config['backup_age_threshold_days']:
            blockers.append({
                'type': 'BACKUP_OUTDATED',
                'message': f"Last backup is {backup_age_days:.0f} days old "
                          f"(threshold: {self.config['backup_age_threshold_days']} days)"
            })
        
        # Check 2: Backup integrity
        if asset['metadata'].get('backup_system'):
            verification = await self.backup.verify_backup(
                asset['metadata']['backup_job_id']
            )
            
            if not verification['success']:
                blockers.append({
                    'type': 'BACKUP_INVALID',
                    'message': f"Backup verification failed: {verification['error']}"
                })
        
        # Check 3: Retention policies
        snapshots_with_holds = await self.storage.list_snapshots_with_retention(
            asset['external_id']
        )
        
        for snapshot in snapshots_with_holds:
            retention_expires = snapshot['retention_expires_at']
            if retention_expires > now():
                days_remaining = (retention_expires - now()).days
                blockers.append({
                    'type': 'RETENTION_HOLD',
                    'message': f"Snapshot retained until {retention_expires} "
                              f"({days_remaining} days remaining)"
                })
        
        # Check 4: Legal holds
        if asset['metadata'].get('legal_hold'):
            blockers.append({
                'type': 'LEGAL_HOLD',
                'message': "Asset under legal hold; requires legal team approval"
            })
        
        # Summary
        if blockers:
            summary = f"{len(blockers)} compliance blocker(s)"
        else:
            summary = "All compliance checks passed"
        
        return {
            'items': blockers,
            'summary': summary,
            'checked_at': now().isoformat()
        }
    
    async def trigger_erase(self, asset, erase_task):
        """Execute secure erase."""
        
        erase_method = erase_task['metadata']['erase_method']
        
        self.logger.info(f"Triggering erase: {asset['name']} via {erase_method}")
        
        try:
            erase_result = await self.storage.secure_erase(
                lun_id=asset['external_id'],
                method=erase_method
            )
            
            # Collect certificate of destruction
            cod = erase_result.get('certificate_of_destruction')
            
            if cod:
                self.state_db.store_evidence(
                    asset_id=asset['id'],
                    evidence_type='COD',
                    content=cod,
                    collected_by='compliance-orchestrator'
                )
            
            # Update asset status
            self.state_db.update_asset(
                asset['id'],
                status='DECOMMISSIONED',
                metadata={
                    **asset.get('metadata', {}),
                    'erased_at': now().isoformat(),
                    'erase_method': erase_method
                }
            )
            
            erase_task['status'] = 'COMPLETED'
            self.state_db.update_task(erase_task)
            
            # Publish event
            self.producer.send('ERASE_COMPLETED', {
                'timestamp': now().isoformat(),
                'asset_id': asset['id'],
                'erase_method': erase_method
            })
            
            self.logger.info(f"Erase completed: {asset['name']}")
        
        except Exception as e:
            self.logger.error(f"Erase failed: {e}", exc_info=True)
            
            erase_task['status'] = 'FAILED'
            erase_task['error'] = str(e)
            self.state_db.update_task(erase_task)
            
            self.producer.send('ERASE_FAILED', {
                'timestamp': now().isoformat(),
                'asset_id': asset['id'],
                'error': str(e)
            })
            
            # Escalate
            await self.escalate(
                priority='HIGH',
                message=f"Erase failed for {asset['name']}: {e}"
            )
    
    def determine_erase_method(self, asset):
        """Determine erase method based on data classification."""
        
        classification = asset['metadata'].get('data_classification', 'INTERNAL')
        
        if classification in ['CONFIDENTIAL', 'RESTRICTED']:
            return 'DOD_5220_22_M'
        else:
            return 'STANDARD_DELETE'
    
    def format_erase_approval_message(self, asset, task):
        """Format approval request for erase."""
        
        erase_method = task['metadata']['erase_method']
        
        return (
            f"Secure erase ready for approval:\n\n"
            f"Asset: {asset['name']} ({asset['size_gb']} GB)\n"
            f"Data classification: {asset['metadata'].get('data_classification', 'INTERNAL')}\n"
            f"Erase method: {erase_method}\n"
            f"Compliance checks: PASSED\n"
            f"\n"
            f"Please review and approve."
        )
    
    async def check_approval_timeouts(self):
        """Check for approval timeouts and escalate."""
        
        pending_approvals = self.state_db.query_approval_requests(status='PENDING')
        
        for approval in pending_approvals:
            if approval['timeout_at'] < now():
                self.logger.warn(f"Approval timeout: {approval['id']}")
                
                approval['status'] = 'TIMEOUT'
                self.state_db.update_approval_request(approval)
                
                await self.escalate(
                    priority='MEDIUM',
                    message=f"Erase approval timed out (no response from "
                           f"{approval['approver']} within 72 hours)"
                )
    
    async def escalate(self, priority, message):
        """Create escalation and notify."""
        
        self.producer.send('ESCALATION_CREATED', {
            'timestamp': now().isoformat(),
            'priority': priority,
            'message': message,
            'agent': 'compliance-orchestrator'
        })
```

### 2.2 Physical Decommission Orchestrator (Central)

```python
class PhysicalOrchestrator:
    """
    Centralized orchestrator for physical decommissioning.
    
    Listens for:
      - ERASE_COMPLETED (check if array is now empty)
    
    Publishes:
      - ARRAY_POWERDOWN_REQUESTED
      - ARRAY_POWERDOWN_CONFIRMED
      - ARRAY_DECOMMISSIONED (final)
    
    Enforces strict governance:
      - Only power down after ALL data is erased
      - Coordinates with DC ops
      - Tracks vendor pickup
    """
    
    def __init__(self, config):
        self.config = config
        self.state_db = PostgresStateDB(config['db'])
        self.audit_log = AuditLog(self.state_db)
        
        self.consumer = KafkaConsumer(
            bootstrap_servers=config['kafka_brokers'],
            group_id='physical-orchestrator',
            auto_offset_reset='earliest'
        )
        self.producer = KafkaProducer(
            bootstrap_servers=config['kafka_brokers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.ticketing = TicketingConnector(config['ticketing'])
        self.messaging = MessagingConnector(config['messaging'])
        self.approval_manager = ApprovalManager(self.state_db, self.audit_log)
        
        self.logger = Logger("PhysicalOrchestrator")
    
    async def run(self):
        """Main loop."""
        
        self.consumer.subscribe(['ERASE_COMPLETED'])
        
        # Check periodically for completed tasks
        asyncio.create_task(self.check_incomplete_tasks())
        
        while True:
            try:
                messages = self.consumer.poll(timeout_ms=1000, max_records=10)
                
                for topic_partition, messages_list in messages.items():
                    for message in messages_list:
                        event = json.loads(message.value)
                        
                        # Check if array is now empty
                        await self.check_if_array_empty()
                        
                        self.consumer.commit()
                
                await asyncio.sleep(60)
            
            except Exception as e:
                self.logger.error(f"Physical orchestrator error: {e}", exc_info=True)
                await asyncio.sleep(10)
    
    async def check_if_array_empty(self):
        """
        Check if ALL assets are decommissioned.
        If yes, initiate array power-down.
        """
        
        all_assets = self.state_db.query_assets()
        decommissioned = [a for a in all_assets if a['status'] == 'DECOMMISSIONED']
        
        if len(decommissioned) == len(all_assets) and len(all_assets) > 0:
            # Array is empty!
            self.logger.info("All assets decommissioned; initiating array power-down")
            
            await self.request_array_powerdown()
    
    async def request_array_powerdown(self):
        """Request DC ops to power down the array."""
        
        # Create task
        task = self.state_db.create_task(
            asset_id=None,  # Array-level
            task_type='POWERDOWN',
            status='AWAITING_APPROVAL',
            metadata={
                'array_id': self.config['array_id'],
                'reason': 'All data migrated and erased'
            }
        )
        
        # Request approval from DC ops
        approval = await self.approval_manager.request_approval(
            task_id=task['id'],
            approver=self.config['dc_ops_contact'],
            message=(
                f"Ready to power down array {self.config['array_name']}.\n"
                f"All data has been migrated and securely erased.\n"
                f"Rack location: {self.config['array_location']}\n"
                f"Please verify the array physically and confirm power-down."
            ),
            timeout_hours=168  # 1 week for physical tasks
        )
        
        if approval['approved']:
            # Wait for DC ops to confirm power-down
            await self.wait_for_powerdown_confirmation()
        else:
            task['status'] = 'BLOCKED'
            task['block_reason'] = approval['reason']
            self.state_db.update_task(task)
            
            await self.escalate(
                priority='MEDIUM',
                message=f"Array power-down approval denied or timed out"
            )
    
    async def wait_for_powerdown_confirmation(self):
        """
        DC ops confirmed power-down. Now handle vendor pickup.
        """
        
        self.logger.info("Array power-down confirmed; handling vendor/disposition")
        
        array_metadata = self.config.get('array_metadata', {})
        lease_id = array_metadata.get('lease_id')
        
        if lease_id:
            # Leased array: contact vendor for pickup
            await self.schedule_vendor_pickup(lease_id)
        else:
            # Owned array: contact disposition team
            await self.arrange_disposition()
    
    async def schedule_vendor_pickup(self, lease_id):
        """Contact vendor to schedule pickup."""
        
        lease_info = self.state_db.get_lease_info(lease_id)
        vendor = lease_info['vendor']
        
        # Create RMA ticket
        rma_ticket = await self.ticketing.create_ticket(
            title=f"Schedule RMA pickup: {self.config['array_name']}",
            description=(
                f"Leased array ready for pickup and return.\n"
                f"Serial number: {self.config['array_serial']}\n"
                f"Lease ID: {lease_id}\n"
                f"Location: {self.config['array_location']}\n"
            ),
            assignee=vendor['contact_email']
        )
        
        self.logger.info(f"Created RMA ticket: {rma_ticket['id']}")
        
        # Request approval from vendor
        approval = await self.approval_manager.request_approval(
            task_id=None,
            approver=vendor['contact_email'],
            message=(
                f"Please schedule pickup of leased array {self.config['array_name']}.\n"
                f"RMA ticket: {rma_ticket['id']}\n"
                f"Location: {self.config['array_location']}"
            ),
            timeout_hours=336  # 2 weeks
        )
        
        if approval['approved']:
            # Pickup scheduled
            self.state_db.update_asset(
                self.config['array_asset_id'],
                status='DECOMMISSIONED',
                metadata={
                    'pickup_confirmed_at': now().isoformat(),
                    'vendor_contact': approval['approver']
                }
            )
            
            self.producer.send('ARRAY_DECOMMISSIONED', {
                'timestamp': now().isoformat(),
                'array_id': self.config['array_id'],
                'disposition': 'VENDOR_PICKUP'
            })
        else:
            await self.escalate(
                priority='MEDIUM',
                message=f"Vendor did not schedule pickup within SLA"
            )
    
    async def arrange_disposition(self):
        """Arrange disposition for owned array (recycling, secondary market, etc.)."""
        
        ticket = await self.ticketing.create_ticket(
            title=f"Arrange disposition: {self.config['array_name']}",
            description=(
                f"Owned array ready for disposition.\n"
                f"Options: Recycling, secondary market, donation\n"
                f"Contact procurement team."
            ),
            assignee=self.config['procurement_contact']
        )
        
        self.logger.info(f"Created disposition ticket: {ticket['id']}")
    
    async def check_incomplete_tasks(self):
        """Periodically check for stalled tasks and escalate if overdue."""
        
        while True:
            await asyncio.sleep(3600)  # Check every hour
            
            incomplete = self.state_db.query_tasks(
                status__in=['AWAITING_APPROVAL', 'IN_PROGRESS'],
                scheduled_for__lt=now() - timedelta(days=1)
            )
            
            for task in incomplete:
                self.logger.warn(f"Task overdue: {task['id']} ({task['task_type']})")
                
                await self.escalate(
                    priority='MEDIUM',
                    message=f"Task {task['task_type']} overdue by "
                           f"{(now() - task['scheduled_for']).days} days"
                )
    
    async def escalate(self, priority, message):
        """Create escalation."""
        
        self.producer.send('ESCALATION_CREATED', {
            'timestamp': now().isoformat(),
            'priority': priority,
            'message': message,
            'agent': 'physical-orchestrator'
        })
```

---

## Message Bus Setup

The message bus (Kafka) is the connective tissue between agents.

### 3.1 Kafka Configuration

```yaml
# kafka-config.yaml

bootstrap_servers:
  - "kafka-1.company.internal:9092"
  - "kafka-2.company.internal:9092"
  - "kafka-3.company.internal:9092"

topics:
  # Observation & inventory
  - name: REFRESH_INVENTORY
    partitions: 1
    replication_factor: 3
    retention_ms: 86400000  # 24 hours
  
  - name: INVENTORY_UPDATED
    partitions: 3
    replication_factor: 3
    retention_ms: 604800000  # 7 days
  
  - name: INVENTORY_SCAN_FAILED
    partitions: 1
    replication_factor: 3
  
  # Migration events
  - name: MIGRATION_APPROVED
    partitions: 5
    replication_factor: 3
  
  - name: MIGRATION_STARTED
    partitions: 5
    replication_factor: 3
  
  - name: MIGRATION_COMPLETED
    partitions: 5
    replication_factor: 3
  
  - name: MIGRATION_FAILED
    partitions: 5
    replication_factor: 3
  
  # Compliance events
  - name: COMPLIANCE_CLEARED
    partitions: 3
    replication_factor: 3
  
  - name: COMPLIANCE_BLOCKED
    partitions: 3
    replication_factor: 3
  
  - name: ERASE_APPROVED
    partitions: 3
    replication_factor: 3
  
  - name: ERASE_COMPLETED
    partitions: 3
    replication_factor: 3
  
  - name: ERASE_FAILED
    partitions: 3
    replication_factor: 3
  
  # Physical events
  - name: ARRAY_POWERDOWN_REQUESTED
    partitions: 1
    replication_factor: 3
  
  - name: ARRAY_POWERDOWN_CONFIRMED
    partitions: 1
    replication_factor: 3
  
  - name: ARRAY_DECOMMISSIONED
    partitions: 1
    replication_factor: 3
  
  # Reporting
  - name: METRICS_UPDATED
    partitions: 1
    replication_factor: 3
  
  - name: REPORT_GENERATED
    partitions: 1
    replication_factor: 3
  
  - name: ALERT
    partitions: 1
    replication_factor: 3
  
  # Escalations & approvals
  - name: ESCALATION_CREATED
    partitions: 3
    replication_factor: 3
  
  - name: APPROVAL_REQUESTED
    partitions: 3
    replication_factor: 3
  
  - name: APPROVAL_RESPONDED
    partitions: 3
    replication_factor: 3

# Consumer groups
consumer_groups:
  migration-agent:
    auto.offset.reset: earliest
    enable.auto.commit: false
  
  compliance-orchestrator:
    auto.offset.reset: earliest
    enable.auto.commit: false
  
  physical-orchestrator:
    auto.offset.reset: earliest
    enable.auto.commit: false
  
  reporting-agent:
    auto.offset.reset: earliest
    enable.auto.commit: false
```

### 3.2 Event Schema (JSON)

```json
// Example: MIGRATION_APPROVED
{
  "event_id": "evt_abc123",
  "timestamp": "2024-01-15T10:00:00Z",
  "event_type": "MIGRATION_APPROVED",
  "asset_id": "asset_xyz",
  "asset_name": "storage-lun-prod-001",
  "target_array": "array-secondary-001",
  "approver": "alice@company.com",
  "approval_timestamp": "2024-01-15T10:00:00Z",
  "suggested_window": {
    "start": "2024-01-20T00:00:00Z",
    "duration_hours": 4,
    "timezone": "UTC"
  }
}

// Example: MIGRATION_COMPLETED
{
  "event_id": "evt_def456",
  "timestamp": "2024-01-20T01:35:00Z",
  "event_type": "MIGRATION_COMPLETED",
  "asset_id": "asset_xyz",
  "asset_name": "storage-lun-prod-001",
  "status": "SUCCESS",
  "duration_seconds": 3300,
  "baseline": {
    "latency_ms": 12.5,
    "iops": 8500,
    "throughput_gbps": 3.2
  },
  "post_migration": {
    "latency_ms": 13.1,
    "iops": 8420,
    "throughput_gbps": 3.19,
    "checks_passed": 3
  }
}

// Example: COMPLIANCE_CLEARED
{
  "event_id": "evt_ghi789",
  "timestamp": "2024-01-20T02:00:00Z",
  "event_type": "COMPLIANCE_CLEARED",
  "asset_id": "asset_xyz",
  "checks": {
    "backup_coverage": "PASS",
    "backup_integrity": "PASS",
    "retention_holds": "PASS",
    "legal_holds": "PASS"
  },
  "erase_method": "DOD_5220_22_M",
  "ready_for_erase": true
}

// Example: ESCALATION_CREATED
{
  "event_id": "evt_jkl012",
  "timestamp": "2024-01-15T14:30:00Z",
  "event_type": "ESCALATION_CREATED",
  "priority": "HIGH",
  "message": "Backup verification failed for storage-lun-prod-002",
  "asset_id": "asset_abc",
  "agent": "compliance-orchestrator",
  "action_required": true,
  "recommended_owner": "backup-team@company.com"
}
```

---

## Coordination Pattern

How do autonomous agents and central orchestrators work together?

### 4.1 Happy Path (Autonomous + Governance)

```
┌─ Autonomous: Inventory Agent ──────────┐
│ 1. Scans storage, VMware, backup       │
│ 2. Discovers 50 LUNs                   │
│ 3. Publishes: INVENTORY_UPDATED        │
└────────────────────────────────────────┘
                    ↓
        ┌────────────────────────────┐
        │ Message Bus               │
        │ INVENTORY_UPDATED         │
        └────────────────────────────┘
                    ↓
┌─ Autonomous: Migration Agent ──────────┐
│ 1. Subscribes to INVENTORY_UPDATED     │
│ 2. Re-computes migration order (topo)  │
│ 3. Waits for MIGRATION_APPROVED events │
└────────────────────────────────────────┘
                    ↓
        ┌─────────────────────────────────┐
        │ PM approves migration wave     │
        │ via web UI (external)          │
        │ Publishes: MIGRATION_APPROVED  │
        └─────────────────────────────────┘
                    ↓
┌─ Autonomous: Migration Agent ──────────┐
│ 1. Receives MIGRATION_APPROVED         │
│ 2. Executes migration (30-60 min)      │
│ 3. Validates post-migration            │
│ 4. Publishes: MIGRATION_COMPLETED      │
└────────────────────────────────────────┘
                    ↓
        ┌────────────────────────────┐
        │ Message Bus               │
        │ MIGRATION_COMPLETED       │
        └────────────────────────────┘
                    ↓
┌─ Central: Compliance Orchestrator ─────┐
│ 1. Receives MIGRATION_COMPLETED        │
│ 2. Checks compliance (sync, <5 min):   │
│    - Backup coverage ✓                 │
│    - Retention holds ✓                 │
│    - Legal holds ✓                     │
│ 3. Requests erase approval from CO     │
│ 4. CO approves (via Slack)             │
│ 5. Triggers erase (via agent call)     │
│ 6. Publishes: ERASE_COMPLETED         │
└────────────────────────────────────────┘
                    ↓
        ┌────────────────────────────┐
        │ Message Bus               │
        │ ERASE_COMPLETED           │
        └────────────────────────────┘
                    ↓
        ┌──────────────────────────────────┐
        │ Central: Physical Orchestrator  │
        │ 1. Checks if array is empty     │
        │ 2. Requests DC ops to power-down│
        │ 3. DC ops confirms (1-7 days)   │
        │ 4. Schedules vendor pickup (2w) │
        │ 5. Publishes: ARRAY_DECOMMISSIONED
        └──────────────────────────────────┘
                    ↓
        ┌────────────────────────────┐
        │ Autonomous: Reporting Agent│
        │ Updates metrics & status   │
        └────────────────────────────┘
```

---

## Deployment

```yaml
# docker-compose.yaml

version: '3.9'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
  
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: decommission
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
  
  inventory-agent:
    build:
      context: .
      dockerfile: agents/inventory.Dockerfile
    environment:
      KAFKA_BROKERS: kafka:9092
      DB_HOST: postgres
      DB_PASSWORD: ${DB_PASSWORD}
    depends_on:
      - kafka
      - postgres
    restart: always
  
  migration-agent:
    build:
      context: .
      dockerfile: agents/migration.Dockerfile
    environment:
      KAFKA_BROKERS: kafka:9092
      DB_HOST: postgres
      DB_PASSWORD: ${DB_PASSWORD}
      MAX_CONCURRENT: 5
    depends_on:
      - kafka
      - postgres
    restart: always
  
  compliance-orchestrator:
    build:
      context: .
      dockerfile: orchestrators/compliance.Dockerfile
    environment:
      KAFKA_BROKERS: kafka:9092
      DB_HOST: postgres
      DB_PASSWORD: ${DB_PASSWORD}
      COMPLIANCE_OFFICER_EMAIL: compliance@company.com
    depends_on:
      - kafka
      - postgres
    restart: always
  
  physical-orchestrator:
    build:
      context: .
      dockerfile: orchestrators/physical.Dockerfile
    environment:
      KAFKA_BROKERS: kafka:9092
      DB_HOST: postgres
      DB_PASSWORD: ${DB_PASSWORD}
      DC_OPS_CONTACT: dcops@company.com
    depends_on:
      - kafka
      - postgres
    restart: always
  
  reporting-agent:
    build:
      context: .
      dockerfile: agents/reporting.Dockerfile
    environment:
      KAFKA_BROKERS: kafka:9092
      DB_HOST: postgres
      DB_PASSWORD: ${DB_PASSWORD}
    depends_on:
      - kafka
      - postgres
    restart: always

volumes:
  postgres_data:
```

---

## Key Advantages of Hybrid Pattern

✓ **Parallelism**: Multiple migrations happen at once (no bottleneck)  
✓ **Speed**: Autonomous agents react to events in real-time  
✓ **Governance**: Compliance and physical operations stay centralized  
✓ **Resilience**: If one agent fails, others continue  
✓ **Scalability**: Add more migration agents to scale parallelism  

This pattern scales to 100+ LUNs without bottlenecks.