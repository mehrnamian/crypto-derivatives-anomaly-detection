targetScope = 'subscription'

@description('Subscription-wide monthly budget in the billing currency. 50 is only the guide suggestion; set your own amount.')
@minValue(1)
param monthlyBudgetAmount int = 50
@description('Owner email for 50%, 80%, and 100% budget notifications.')
param alertEmail string
@description('First day of the current billing month, YYYY-MM-DD.')
param periodStartDate string
@description('Budget end date, YYYY-MM-DD.')
param periodEndDate string

resource subscriptionBudget 'Microsoft.Consumption/budgets@2023-05-01' = {
  name: 'derivs-anomaly-monthly'
  properties: {
    category: 'Cost'
    amount: monthlyBudgetAmount
    timeGrain: 'Monthly'
    timePeriod: {
      startDate: periodStartDate
      endDate: periodEndDate
    }
    notifications: {
      threshold50: {
        enabled: true
        operator: 'GreaterThanOrEqualTo'
        threshold: 50
        contactEmails: [ alertEmail ]
        contactRoles: []
        contactGroups: []
      }
      threshold80: {
        enabled: true
        operator: 'GreaterThanOrEqualTo'
        threshold: 80
        contactEmails: [ alertEmail ]
        contactRoles: []
        contactGroups: []
      }
      threshold100: {
        enabled: true
        operator: 'GreaterThanOrEqualTo'
        threshold: 100
        contactEmails: [ alertEmail ]
        contactRoles: []
        contactGroups: []
      }
    }
  }
}
