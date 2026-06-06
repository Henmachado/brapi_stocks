import constants

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def read_raw_layer_json(spark: SparkSession, file_name: str) -> DataFrame:
    return spark.read.option("multiline", "true").json(f"{constants.RAW_LAYER}/{file_name}")


def create_activetickers_df(spark: SparkSession, file_name: str) -> DataFrame:
    return read_raw_layer_json(spark=spark, file_name=file_name)


def create_defaultkeystatistics_df(spark: SparkSession, file_name: str) -> DataFrame:
    return read_raw_layer_json(spark=spark, file_name=file_name)


def create_balancesheethistoryquarterly_df(spark: SparkSession, file_name: str) -> DataFrame:
    return (
        read_raw_layer_json(spark=spark, file_name=file_name)
        .select("symbol", F.explode("balanceSheetHistoryQuarterly").alias("array"))
        .select(
            F.col("symbol").alias("stock"),
            F.col("array.type").alias("type"),
            F.col("array.endDate").alias("endDate"),
            F.col("array.cash").alias("cash"),
            F.col("array.shortTermInvestments").alias("shortTermInvestments"),
            F.col("array.netReceivables").alias("netReceivables"),
            F.col("array.inventory").alias("inventory"),
            F.col("array.otherCurrentAssets").alias("otherCurrentAssets"),
            F.col("array.totalCurrentAssets").alias("totalCurrentAssets"),
            F.col("array.longTermInvestments").alias("longTermInvestments"),
            F.col("array.propertyPlantEquipment").alias("propertyPlantEquipment"),
            F.col("array.otherAssets").alias("otherAssets"),
            F.col("array.totalAssets").alias("totalAssets"),
            F.col("array.accountsPayable").alias("accountsPayable"),
            F.col("array.shortLongTermDebt").alias("shortLongTermDebt"),
            F.col("array.otherCurrentLiab").alias("otherCurrentLiab"),
            F.col("array.longTermDebt").alias("longTermDebt"),
            F.col("array.otherLiab").alias("otherLiab"),
            F.col("array.totalCurrentLiabilities").alias("totalCurrentLiabilities"),
            F.col("array.totalLiab").alias("totalLiab"),
            F.col("array.commonStock").alias("commonStock"),
            F.col("array.retainedEarnings").alias("retainedEarnings"),
            F.col("array.treasuryStock").alias("treasuryStock"),
            F.col("array.otherStockholderEquity").alias("otherStockholderEquity"),
            F.col("array.totalStockholderEquity").alias("totalStockholderEquity"),
            F.col("array.netTangibleAssets").alias("netTangibleAssets"),
            F.col("array.goodWill").alias("goodWill"),
            F.col("array.intangibleAssets").alias("intangibleAssets"),
            F.col("array.deferredLongTermAssetCharges").alias("deferredLongTermAssetCharges"),
            F.col("array.deferredLongTermLiab").alias("deferredLongTermLiab"),
            F.col("array.minorityInterest").alias("minorityInterest"),
            F.col("array.capitalSurplus").alias("capitalSurplus"),
            F.col("array.financialAssets").alias("financialAssets"),
            F.col("array.centralBankCompulsoryDeposit").alias("centralBankCompulsoryDeposit"),
            F.col("array.financialAssetsMeasuredAtFairValueThroughProfitOrLoss").alias(
                "financialAssetsMeasuredAtFairValueThroughProfitOrLoss"),
            F.col("array.currentAndDeferredTaxes").alias("currentAndDeferredTaxes"),
            F.col("array.investments").alias("investments"),
            F.col("array.financialAssetsFVThroughOCI").alias("financialAssetsFVThroughOCI"),
            F.col("array.financialAssetsAtAmortizedCost").alias("financialAssetsAtAmortizedCost"),
            F.col("array.accountsReceivableFromClients").alias("accountsReceivableFromClients"),
            F.col("array.otherAccountsReceivable").alias("otherAccountsReceivable"),
            F.col("array.biologicalAssets").alias("biologicalAssets"),
            F.col("array.taxesToRecover").alias("taxesToRecover"),
            F.col("array.prepaidExpenses").alias("prepaidExpenses"),
            F.col("array.longTermAssets").alias("longTermAssets"),
            F.col("array.longTermRealizableAssets").alias("longTermRealizableAssets"),
            F.col("array.longTermReceivables").alias("longTermReceivables"),
            F.col("array.longTermAccountsReceivableFromClients").alias("longTermAccountsReceivableFromClients"),
            F.col("array.longTermInventory").alias("longTermInventory"),
            F.col("array.longTermBiologicalAssets").alias("longTermBiologicalAssets"),
            F.col("array.longTermDeferredTaxes").alias("longTermDeferredTaxes"),
            F.col("array.longTermPrepaidExpenses").alias("longTermPrepaidExpenses"),
            F.col("array.creditsWithRelatedParties").alias("creditsWithRelatedParties"),
            F.col("array.shareholdings").alias("shareholdings"),
            F.col("array.investmentProperties").alias("investmentProperties"),
            F.col("array.otherLongTermReceivables").alias("otherLongTermReceivables"),
            F.col("array.otherNonCurrentAssets").alias("otherNonCurrentAssets"),
            F.col("array.creditsFromOperations").alias("creditsFromOperations"),
            F.col("array.insuranceAndReinsurance").alias("insuranceAndReinsurance"),
            F.col("array.complementaryPension").alias("complementaryPension"),
            F.col("array.securitiesAndCreditsReceivable").alias("securitiesAndCreditsReceivable"),
            F.col("array.otherValuesAndAssets").alias("otherValuesAndAssets"),
            F.col("array.compulsoryLoansAndDeposits").alias("compulsoryLoansAndDeposits"),
            F.col("array.deferredSellingExpenses").alias("deferredSellingExpenses"),
            F.col("array.nonCurrentAssets").alias("nonCurrentAssets"),
            F.col("array.longTermFinancialInvestmentsMeasuredAtFairValueThroughIncome").alias(
                "longTermFinancialInvestmentsMeasuredAtFairValueThroughIncome"),
            F.col("array.financialInvestmentsFVThroughOCI").alias("financialInvestmentsFVThroughOCI"),
            F.col("array.financialInvestmentsMeasuredAtAmortizedCost").alias(
                "financialInvestmentsMeasuredAtAmortizedCost"),
            F.col("array.intangibleAsset").alias("intangibleAsset"),
            F.col("array.deferredTaxes").alias("deferredTaxes"),
            F.col("array.capitalization").alias("capitalization"),
            F.col("array.otherOperations").alias("otherOperations"),
            F.col("array.financialLiabilitiesMeasuredAtFairValueThroughIncome").alias(
                "financialLiabilitiesMeasuredAtFairValueThroughIncome"),
            F.col("array.financialLiabilitiesAtAmortizedCost").alias("financialLiabilitiesAtAmortizedCost"),
            F.col("array.provisions").alias("provisions"),
            F.col("array.taxLiabilities").alias("taxLiabilities"),
            F.col("array.otherLiabilities").alias("otherLiabilities"),
            F.col("array.shareholdersEquity").alias("shareholdersEquity"),
            F.col("array.controllerShareholdersEquity").alias("controllerShareholdersEquity"),
            F.col("array.nonControllingShareholdersEquity").alias("nonControllingShareholdersEquity"),
            F.col("array.realizedShareCapital").alias("realizedShareCapital"),
            F.col("array.capitalReserves").alias("capitalReserves"),
            F.col("array.revaluationReserves").alias("revaluationReserves"),
            F.col("array.profitReserves").alias("profitReserves"),
            F.col("array.accumulatedProfitsOrLosses").alias("accumulatedProfitsOrLosses"),
            F.col("array.equityValuationAdjustments").alias("equityValuationAdjustments"),
            F.col("array.cumulativeConversionAdjustments").alias("cumulativeConversionAdjustments"),
            F.col("array.otherComprehensiveResults").alias("otherComprehensiveResults"),
            F.col("array.currentLiabilities").alias("currentLiabilities"),
            F.col("array.socialAndLaborObligations").alias("socialAndLaborObligations"),
            F.col("array.providers").alias("providers"),
            F.col("array.nationalSuppliers").alias("nationalSuppliers"),
            F.col("array.foreignSuppliers").alias("foreignSuppliers"),
            F.col("array.taxObligations").alias("taxObligations"),
            F.col("array.loansAndFinancing").alias("loansAndFinancing"),
            F.col("array.loansAndFinancingInNationalCurrency").alias("loansAndFinancingInNationalCurrency"),
            F.col("array.loansAndFinancingInForeignCurrency").alias("loansAndFinancingInForeignCurrency"),
            F.col("array.debentures").alias("debentures"),
            F.col("array.leaseFinancing").alias("leaseFinancing"),
            F.col("array.otherObligations").alias("otherObligations"),
            F.col("array.otherCurrentLiabilities").alias("otherCurrentLiabilities"),
            F.col("array.nonCurrentLiabilities").alias("nonCurrentLiabilities"),
            F.col("array.longTermLoansAndFinancing").alias("longTermLoansAndFinancing"),
            F.col("array.longTermLoansAndFinancingInNationalCurrency").alias(
                "longTermLoansAndFinancingInNationalCurrency"),
            F.col("array.longTermLoansAndFinancingInForeignCurrency").alias("longTermLoansAndFinancingInForeignCurrency"),
            F.col("array.longTermDebentures").alias("longTermDebentures"),
            F.col("array.longTermLeaseFinancing").alias("longTermLeaseFinancing"),
            F.col("array.otherLongTermObligations").alias("otherLongTermObligations"),
            F.col("array.longTermProvisions").alias("longTermProvisions"),
            F.col("array.otherNonCurrentLiabilities").alias("otherNonCurrentLiabilities"),
            F.col("array.profitsAndRevenuesToBeAppropriated").alias("profitsAndRevenuesToBeAppropriated"),
            F.col("array.debitsFromOperations").alias("debitsFromOperations"),
            F.col("array.debitsFromInsuranceAndReinsurance").alias("debitsFromInsuranceAndReinsurance"),
            F.col("array.debitsFromComplementaryPension").alias("debitsFromComplementaryPension"),
            F.col("array.thirdPartyDeposits").alias("thirdPartyDeposits"),
            F.col("array.technicalProvisions").alias("technicalProvisions"),
            F.col("array.otherDebits").alias("otherDebits"),
            F.col("array.longTermLiabilities").alias("longTermLiabilities"),
            F.col("array.longTermAccountsPayable").alias("longTermAccountsPayable"),
            F.col("array.longTermDebitsFromOperations").alias("longTermDebitsFromOperations"),
            F.col("array.longTermTechnicalProvisions").alias("longTermTechnicalProvisions"),
            F.col("array.longTermInsuranceAndReinsurance").alias("longTermInsuranceAndReinsurance"),
            F.col("array.longTermComplementaryPension").alias("longTermComplementaryPension"),
            F.col("array.longTermCapitalization").alias("longTermCapitalization"),
            F.col("array.otherLongTermProvisions").alias("otherLongTermProvisions"),
            F.col("array.debitsFromCapitalization").alias("debitsFromCapitalization"),
            F.col("array.debitsFromOtherOperations").alias("debitsFromOtherOperations"),
            F.col("array.otherProvisions").alias("otherProvisions"),
            F.col("array.advanceForFutureCapitalIncrease").alias("advanceForFutureCapitalIncrease"),
        )
    )


def create_cashflowhistoryquarterly_df(spark: SparkSession, file_name: str) -> DataFrame:
    return (
        read_raw_layer_json(spark=spark, file_name=file_name)
        .select("symbol", F.explode("cashflowHistoryQuarterly").alias("array"))
        .select(
            F.col("symbol").alias("stock"),
            F.col("array.type").alias("type"),
            F.col("array.endDate").alias("endDate"),
            F.col("array.operatingCashFlow").alias("operatingCashFlow"),
            F.col("array.incomeFromOperations").alias("incomeFromOperations"),
            F.col("array.netIncomeBeforeTaxes").alias("netIncomeBeforeTaxes"),
            F.col("array.adjustmentsToProfitOrLoss").alias("adjustmentsToProfitOrLoss"),
            F.col("array.changesInAssetsAndLiabilities").alias("changesInAssetsAndLiabilities"),
            F.col("array.otherOperatingActivities").alias("otherOperatingActivities"),
            F.col("array.cashGeneratedInOperations").alias("cashGeneratedInOperations"),
            F.col("array.investmentCashFlow").alias("investmentCashFlow"),
            F.col("array.financingCashFlow").alias("financingCashFlow"),
            F.col("array.exchangeVariationWithoutCash").alias("exchangeVariationWithoutCash"),
            F.col("array.foreignExchangeRateWithoutCash").alias("foreignExchangeRateWithoutCash"),
            F.col("array.increaseOrDecreaseInCash").alias("increaseOrDecreaseInCash"),
            F.col("array.initialCashBalance").alias("initialCashBalance"),
            F.col("array.finalCashBalance").alias("finalCashBalance"),
            F.col("array.freeCashFlow").alias("freeCashFlow"),
        )
    )


def create_defaultkeystatisticshistoryquarterly_df(spark: SparkSession, file_name: str) -> DataFrame:
    return (
        read_raw_layer_json(spark=spark, file_name=file_name)
        .select("symbol", F.explode("defaultKeyStatisticsHistoryQuarterly").alias("array"))
        .select(
            F.col("symbol").alias("stock"),
            F.col("array.type").alias("type"),
            F.col("array.endDate").alias("endDate"),
            F.col("array.price").alias("price"),
            F.col("array.enterpriseValue").alias("enterpriseValue"),
            F.col("array.forwardPE").alias("forwardPE"),
            F.col("array.profitMargins").alias("profitMargins"),
            F.col("array.sharesOutstanding").alias("sharesOutstanding"),
            F.col("array.bookValue").alias("bookValue"),
            F.col("array.priceToBook").alias("priceToBook"),
            F.col("array.earningsQuarterlyGrowth").alias("earningsQuarterlyGrowth"),
            F.col("array.netIncomeToCommon").alias("netIncomeToCommon"),
            F.col("array.trailingEps").alias("trailingEps"),
            F.col("array.pegRatio").alias("pegRatio"),
            F.col("array.enterpriseToRevenue").alias("enterpriseToRevenue"),
            F.col("array.enterpriseToEbitda").alias("enterpriseToEbitda"),
            F.col("array.lastDividendValue").alias("lastDividendValue"),
            F.col("array.lastDividendDate").alias("lastDividendDate"),
            F.col("array.marketCap").alias("marketCap"),
            F.col("array.trailingPE").alias("trailingPE"),
            F.col("array.dividendYield").alias("dividendYield"),
            F.col("array.yield").alias("yield"),
            F.col("array.earningsPerShare").alias("earningsPerShare"),
        )
    )


def create_incomestatementhistoryquarterly_df(spark: SparkSession, file_name: str) -> DataFrame:
    return (
        read_raw_layer_json(spark=spark, file_name=file_name)
        .select("symbol", F.explode("incomeStatementHistoryQuarterly").alias("array"))
        .select(
            F.col("symbol").alias("stock"),
            F.col("array.type").alias("type"),
            F.col("array.endDate").alias("endDate"),
            F.col("array.totalRevenue").alias("totalRevenue"),
            F.col("array.costOfRevenue").alias("costOfRevenue"),
            F.col("array.grossProfit").alias("grossProfit"),
            F.col("array.researchDevelopment").alias("researchDevelopment"),
            F.col("array.sellingGeneralAdministrative").alias("sellingGeneralAdministrative"),
            F.col("array.nonRecurring").alias("nonRecurring"),
            F.col("array.otherOperatingExpenses").alias("otherOperatingExpenses"),
            F.col("array.totalOperatingExpenses").alias("totalOperatingExpenses"),
            F.col("array.operatingIncome").alias("operatingIncome"),
            F.col("array.totalOtherIncomeExpenseNet").alias("totalOtherIncomeExpenseNet"),
            F.col("array.ebit").alias("ebit"),
            F.col("array.interestExpense").alias("interestExpense"),
            F.col("array.incomeBeforeTax").alias("incomeBeforeTax"),
            F.col("array.incomeTaxExpense").alias("incomeTaxExpense"),
            F.col("array.minorityInterest").alias("minorityInterest"),
            F.col("array.netIncomeFromContinuingOps").alias("netIncomeFromContinuingOps"),
            F.col("array.discontinuedOperations").alias("discontinuedOperations"),
            F.col("array.extraordinaryItems").alias("extraordinaryItems"),
            F.col("array.effectOfAccountingCharges").alias("effectOfAccountingCharges"),
            F.col("array.otherItems").alias("otherItems"),
            F.col("array.netIncome").alias("netIncome"),
            F.col("array.netIncomeApplicableToCommonShares").alias("netIncomeApplicableToCommonShares"),
            F.col("array.salesExpenses").alias("salesExpenses"),
            F.col("array.lossesDueToNonRecoverabilityOfAssets").alias("lossesDueToNonRecoverabilityOfAssets"),
            F.col("array.otherOperatingIncome").alias("otherOperatingIncome"),
            F.col("array.equityIncomeResult").alias("equityIncomeResult"),
            F.col("array.financialResult").alias("financialResult"),
            F.col("array.financialIncome").alias("financialIncome"),
            F.col("array.financialExpenses").alias("financialExpenses"),
            F.col("array.currentTaxes").alias("currentTaxes"),
            F.col("array.deferredTaxes").alias("deferredTaxes"),
            F.col("array.incomeBeforeStatutoryParticipationsAndContributions").alias(
                "incomeBeforeStatutoryParticipationsAndContributions"),
            F.col("array.basicEarningsPerCommonShare").alias("basicEarningsPerCommonShare"),
            F.col("array.dilutedEarningsPerCommonShare").alias("dilutedEarningsPerCommonShare"),
            F.col("array.basicEarningsPerPreferredShare").alias("basicEarningsPerPreferredShare"),
            F.col("array.profitSharingAndStatutoryContributions").alias("profitSharingAndStatutoryContributions"),
            F.col("array.dilutedEarningsPerPreferredShare").alias("dilutedEarningsPerPreferredShare"),
            F.col("array.claimsAndOperationsCosts").alias("claimsAndOperationsCosts"),
            F.col("array.administrativeCosts").alias("administrativeCosts"),
            F.col("array.otherOperatingIncomeAndExpenses").alias("otherOperatingIncomeAndExpenses"),
            F.col("array.earningsPerShare").alias("earningsPerShare"),
            F.col("array.basicEarningsPerShare").alias("basicEarningsPerShare"),
            F.col("array.dilutedEarningsPerShare").alias("dilutedEarningsPerShare"),
            F.col("array.insuranceOperations").alias("insuranceOperations"),
            F.col("array.reinsuranceOperations").alias("reinsuranceOperations"),
            F.col("array.complementaryPensionOperations").alias("complementaryPensionOperations"),
            F.col("array.capitalizationOperations").alias("capitalizationOperations"),
            F.col("array.cleanEbit").alias("cleanEbit"),
            F.col("array.cleanEbitda").alias("cleanEbitda"),
            F.col("array.cleanNopat").alias("cleanNopat"),
            F.col("array.cleanNetIncome").alias("cleanNetIncome"),
        )
    )


def create_financialdatahistoryquarterly_df(spark: SparkSession, file_name: str) -> DataFrame:
    return (
        read_raw_layer_json(spark=spark, file_name=file_name)
        .select("symbol", F.explode("financialDataHistoryQuarterly").alias("array"))
        .select(
            F.col("symbol").alias("stock"),
            F.col("array.type").alias("type"),
            F.col("array.endDate").alias("endDate"),
            F.col("array.currentPrice").alias("currentPrice"),
            F.col("array.totalCash").alias("totalCash"),
            F.col("array.totalCashPerShare").alias("totalCashPerShare"),
            F.col("array.ebitda").alias("ebitda"),
            F.col("array.totalDebt").alias("totalDebt"),
            F.col("array.quickRatio").alias("quickRatio"),
            F.col("array.currentRatio").alias("currentRatio"),
            F.col("array.totalRevenue").alias("totalRevenue"),
            F.col("array.debtToEquity").alias("debtToEquity"),
            F.col("array.revenuePerShare").alias("revenuePerShare"),
            F.col("array.returnOnAssets").alias("returnOnAssets"),
            F.col("array.returnOnEquity").alias("returnOnEquity"),
            F.col("array.grossProfits").alias("grossProfits"),
            F.col("array.freeCashflow").alias("freeCashflow"),
            F.col("array.operatingCashflow").alias("operatingCashflow"),
            F.col("array.earningsGrowth").alias("earningsGrowth"),
            F.col("array.revenueGrowth").alias("revenueGrowth"),
            F.col("array.earningsGrowthAnnual").alias("earningsGrowthAnnual"),
            F.col("array.revenueGrowthAnnual").alias("revenueGrowthAnnual"),
            F.col("array.grossMargins").alias("grossMargins"),
            F.col("array.ebitdaMargins").alias("ebitdaMargins"),
            F.col("array.operatingMargins").alias("operatingMargins"),
            F.col("array.profitMargins").alias("profitMargins"),
            F.col("array.financialCurrency").alias("financialCurrency"),
        )
    )


def create_valueaddedhistoryquarterly_df(spark: SparkSession, file_name: str) -> DataFrame:
    return (
        read_raw_layer_json(spark=spark, file_name=file_name)
        .select("symbol", F.explode("valueAddedHistoryQuarterly").alias("array"))
        .select(
            F.col("symbol").alias("stock"),
            F.col("array.type").alias("type"),
            F.col("array.endDate").alias("endDate"),
            F.col("array.revenue").alias("revenue"),
            F.col("array.productSales").alias("productSales"),
            F.col("array.otherRevenues").alias("otherRevenues"),
            F.col("array.constructionOfOwnAssets").alias("constructionOfOwnAssets"),
            F.col("array.provisionOrReversalOfDoubtfulAccounts").alias("provisionOrReversalOfDoubtfulAccounts"),
            F.col("array.suppliesPurchasedFromThirdParties").alias("suppliesPurchasedFromThirdParties"),
            F.col("array.costsWithProductsSold").alias("costsWithProductsSold"),
            F.col("array.thirdPartyMaterialsAndServices").alias("thirdPartyMaterialsAndServices"),
            F.col("array.lossOrRecoveryOfAssets").alias("lossOrRecoveryOfAssets"),
            F.col("array.otherSupplies").alias("otherSupplies"),
            F.col("array.grossAddedValue").alias("grossAddedValue"),
            F.col("array.retentions").alias("retentions"),
            F.col("array.depreciationAndAmortization").alias("depreciationAndAmortization"),
            F.col("array.otherRetentions").alias("otherRetentions"),
            F.col("array.netAddedValue").alias("netAddedValue"),
            F.col("array.netAddedValueProduced").alias("netAddedValueProduced"),
            F.col("array.addedValueReceivedOnTransfer").alias("addedValueReceivedOnTransfer"),
            F.col("array.addedValueReceivedByTransfer").alias("addedValueReceivedByTransfer"),
            F.col("array.equityIncomeResult").alias("equityIncomeResult"),
            F.col("array.financialIncome").alias("financialIncome"),
            F.col("array.otherValuesReceivedOnTransfer").alias("otherValuesReceivedOnTransfer"),
            F.col("array.otherValuesReceivedByTransfer").alias("otherValuesReceivedByTransfer"),
            F.col("array.addedValueToDistribute").alias("addedValueToDistribute"),
            F.col("array.totalAddedValueToDistribute").alias("totalAddedValueToDistribute"),
            F.col("array.distributionOfAddedValue").alias("distributionOfAddedValue"),
            F.col("array.teamRemuneration").alias("teamRemuneration"),
            F.col("array.taxes").alias("taxes"),
            F.col("array.federalTaxes").alias("federalTaxes"),
            F.col("array.stateTaxes").alias("stateTaxes"),
            F.col("array.municipalTaxes").alias("municipalTaxes"),
            F.col("array.remunerationOfThirdPartyCapitals").alias("remunerationOfThirdPartyCapitals"),
            F.col("array.equityRemuneration").alias("equityRemuneration"),
            F.col("array.ownEquityRemuneration").alias("ownEquityRemuneration"),
            F.col("array.interestOnOwnEquity").alias("interestOnOwnEquity"),
            F.col("array.dividends").alias("dividends"),
            F.col("array.retainedEarningsOrLoss").alias("retainedEarningsOrLoss"),
            F.col("array.nonControllingShareOfRetainedEarnings").alias("nonControllingShareOfRetainedEarnings"),
            F.col("array.otherDistributions").alias("otherDistributions"),
            F.col("array.financialIntermediationRevenue").alias("financialIntermediationRevenue"),
            F.col("array.revenueFromTheProvisionOfServices").alias("revenueFromTheProvisionOfServices"),
            F.col("array.provisionOrReversalOfExpectedCreditRiskLosses").alias(
                "provisionOrReversalOfExpectedCreditRiskLosses"),
            F.col("array.financialIntermediationExpenses").alias("financialIntermediationExpenses"),
            F.col("array.materialsEnergyAndOthers").alias("materialsEnergyAndOthers"),
            F.col("array.services").alias("services"),
            F.col("array.lossOrRecoveryOfAssetValues").alias("lossOrRecoveryOfAssetValues"),
            F.col("array.thirdPartyEquityRemuneration").alias("thirdPartyEquityRemuneration"),
            F.col("array.insuranceOperationsRevenue").alias("insuranceOperationsRevenue"),
            F.col("array.complementaryPensionOperationsRevenue").alias("complementaryPensionOperationsRevenue"),
            F.col("array.feesRevenue").alias("feesRevenue"),
            F.col("array.variationsOfTechnicalProvisions").alias("variationsOfTechnicalProvisions"),
            F.col("array.insuranceOperationsVariations").alias("insuranceOperationsVariations"),
            F.col("array.pensionOperationsVariations").alias("pensionOperationsVariations"),
            F.col("array.otherVariations").alias("otherVariations"),
            F.col("array.netOperatingRevenue").alias("netOperatingRevenue"),
            F.col("array.claimsAndBenefits").alias("claimsAndBenefits"),
            F.col("array.variationInDeferredSellingExpenses").alias("variationInDeferredSellingExpenses"),
            F.col("array.resultsOfCededReinsuranceOperations").alias("resultsOfCededReinsuranceOperations"),
            F.col("array.resultOfCoinsuranceOperationsAssigned").alias("resultOfCoinsuranceOperationsAssigned"),
        )
    )