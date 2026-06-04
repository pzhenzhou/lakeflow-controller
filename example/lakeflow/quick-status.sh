#!/bin/bash

# Quick status check for workflow-on-workflow chain
NAMESPACE="lakeflow-controller-test"

echo "🔍 Quick Status Check - Workflow Chain"
echo "========================================"
echo ""

# Current time
echo "⏰ Current Time:"
date -u +'  %Y-%m-%d %H:%M:%S UTC'
echo ""

# LakeFlows
echo "📋 LakeFlows:"
kubectl get lakeflow -n "$NAMESPACE" -o custom-columns=NAME:.metadata.name,AGE:.metadata.creationTimestamp
echo ""

# CronWorkflow
echo "⏱️  CronWorkflow:"
kubectl get cronworkflow -n "$NAMESPACE" -o custom-columns=NAME:.metadata.name,SCHEDULE:.spec.schedule,SUSPEND:.spec.suspend,SUCCEEDED:.status.succeeded,FAILED:.status.failed,AGE:.metadata.creationTimestamp
echo ""

# Workflow Instances
echo "🔄 Workflow Instances:"
kubectl get workflows -n "$NAMESPACE" --sort-by=.metadata.creationTimestamp -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,STARTED:.status.startedAt,FINISHED:.status.finishedAt
echo ""

# EventBus
echo "🚌 EventBus:"
kubectl get eventbus -n "$NAMESPACE" -o custom-columns=NAME:.metadata.name,TYPE:.spec.jetstream.version,AGE:.metadata.creationTimestamp
echo "   JetStream Pods:"
kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=eventbus -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,AGE:.metadata.creationTimestamp | sed 's/^/   /'
echo ""

# EventSource
echo "📡 EventSource:"
kubectl get eventsource -n "$NAMESPACE" -o custom-columns=NAME:.metadata.name,AGE:.metadata.creationTimestamp
echo "   EventSource Pods:"
kubectl get pods -n "$NAMESPACE" -l eventsource-name -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,AGE:.metadata.creationTimestamp 2>/dev/null | sed 's/^/   /' || echo "   None"
echo ""

# Sensor
echo "🎯 Sensor:"
kubectl get sensor -n "$NAMESPACE" -o custom-columns=NAME:.metadata.name,AGE:.metadata.creationTimestamp
echo "   Sensor Pods:"
kubectl get pods -n "$NAMESPACE" -l sensor-name -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,AGE:.metadata.creationTimestamp 2>/dev/null | sed 's/^/   /' || echo "   None"
echo ""

echo "========================================"
echo "💡 Tips:"
echo "  - Run full verification: ./verify-workflow-chain.sh"
echo "  - Watch workflows: kubectl get workflows -n $NAMESPACE -w"
echo "  - Check logs: kubectl logs -n $NAMESPACE -l workflows.argoproj.io/workflow=<workflow-name>"

