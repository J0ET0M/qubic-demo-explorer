<script setup lang="ts">
import { Vote } from 'lucide-vue-next'
import ProposalsTab from '~/components/analytics/ProposalsTab.vue'

useHead({ title: 'On-chain Proposals' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Vote class="h-5 w-5 text-accent" />
      On-chain Proposals
    </h1>
    <p class="text-sm text-muted -mt-2">
      SetProposal + Vote transactions submitted to the
      <strong>GQMPROP</strong> (General Quorum Proposal) and
      <strong>CCF</strong> (Computor Controlled Fund) contracts, with per-epoch tallies,
      voting-time clustering, and owner-level voting breakdowns.
    </p>

    <Suspense>
      <template #default>
        <ProposalsTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading proposals data...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
