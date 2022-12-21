import org.apache.spark.sql.functions.{col, explode, first, monotonically_increasing_id, spark_partition_id, when}
import org.apache.spark.sql.types.{DataTypes, DoubleType, LongType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.io._
import scala.io.BufferedSource
import scala.io.Source.fromFile
import scala.language.postfixOps

object SqlToJson {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("spark_local")
      .getOrCreate()

    import spark.implicits._

//    val columns = Seq("epk_id","calibrated_score_array")
//    val data = Seq(
//      (545863491123L, {"20000"; 20.43}),
//      (545863491123L, {"20000"; 20.43}),
//      (545863491123L, {"20000"; 20.43})
//    )

    var structureData = Seq(
      Row(545863491123L, Map("A07.01591.SberSpasibo_turn_off" -> 20.42, "A23.01639.Exchange_of_coins_for_banknotes" -> 20.42)),
      Row(545863491124L, Map("A23.01639.Exchange_of_coins_for_banknotes" -> 20.42)),
      Row(545863491125L, Map("A02.01575.Reasons_for_loan_arrears" -> 20.42)),
      Row(545863491126L, Map("A31.12685.Bankruptcy" -> 20.42, "brak_osnovnoy_infoquiz" -> 20.42)),
    )

    val mapType = DataTypes.createMapType(StringType,DoubleType)

    val arrayStructureSchema = new StructType()
      .add("epk_id", LongType)
      .add("calibrated_score_array", mapType)

    val mapTypeDF = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData),arrayStructureSchema)
    mapTypeDF.printSchema()
    mapTypeDF.show()

//    val newDF = mapTypeDF.select(
//      col("epk_id"),
//      col("calibrated_score_array").cast("array<struct<key:string,value:double>>"))
//    newDF.printSchema()
//    newDF.show()

    mapTypeDF.createOrReplaceTempView("test_table")

//    val explodeDF = mapTypeDF.select($"epk_id", explode($"calibrated_score_array"))
//      .withColumn("system_name", when(col("key") === "A07.01591.SberSpasibo_turn_off", "SberSpasibo_turn_off")
//        when(col("key") === "A23.01639.Exchange_of_coins_for_banknotes", "Exchange_of_coins_for_banknotes")
//        when(col("key") === "A02.01575.Reasons_for_loan_arrears", "loans_general_info")
//        when(col("key") === "A31.12685.Bankruptcy", "Bankruptcy")
//        when(col("key") === "brak_osnovnoy_infoquiz", "gosuslugi")
//      )
//      .withColumnRenamed("key","intent")
//      .withColumnRenamed("value","score")
////      .groupBy("epk_id")
////      .pivot("key")
////      .agg(first("value")).show()
//    explodeDF.printSchema()
//    explodeDF.show()

//    val sqlQuery: String = "SELECT epk_id, explode(calibrated_score_array) AS (intent, score), CASE WHEN intent = 'A23.01639.Exchange_of_coins_for_banknotes' THEN 'Exchange_of_coins_for_banknotes' ELSE '' END AS system_name FROM test_table"
//    spark.sql(sqlQuery).show()
//
//    val sqlQueryCheck: String = "SELECT a.epk_id, array(named_struct('intent', a.intent, 'score', a.score, 'system_name', a.system_name)) as product_intents from (SELECT epk_id, explode(calibrated_score_array) AS (intent, score), CASE WHEN intent = 'A23.01639.Exchange_of_coins_for_banknotes' THEN 'Exchange_of_coins_for_banknotes' ELSE '' END AS system_name FROM test_table) as a"
//    spark.sql(sqlQueryCheck).write.json("src/main/recources/result")

//    val sqlQuery: String = "select array_of_structs.epk_id, transform(array_of_structs.calibrated_score_array, " +
//      "x -> case when x.key = 'A23.01639.Exchange_of_coins_for_banknotes' then named_struct('intent', x.key, 'score', x.value, 'sys_name', 'Exchange_of_coins_for_banknotes')" +
//      "else named_struct('intent', x.key, 'score', x.value, 'sys_name', '')" +
//      ")" +
//      "from (select epk_id, calibrated_score_array from test_table) as array_of_structs" +
//      ""

//    val sqlQuery: String = "select epk_id, transform(calibrated_score_array, x -> case " +
//                  "when x.key = 'A23.01639.Exchange_of_coins_for_banknotes' then named_struct('key', x.key, 'value', x.value, 'sys_name', 'Exchange_of_coins_for_banknotes') " +
//                  "else named_struct('intent', x.key, 'score', x.value, 'sys_name', '')" +
//                  "end) as product_intents " +
//                  "from test_table"

    spark.sql("""
          SELECT a.epk_id, collect_list(named_struct('intent', a.intent, 'score', a.score, 'system_name', a.system_name)) as product_intents
          from (SELECT epk_id, explode(calibrated_score_array) AS (intent, score),
                CASE
                    WHEN intent = 'A23.01639.Exchange_of_coins_for_banknotes' THEN 'Exchange_of_coins_for_banknotes'
                    ELSE '' END AS system_name FROM test_table) as a
        group by epk_id
          """).show()

    spark.sql("""
          SELECT a.epk_id, collect_list(named_struct('intent', a.intent, 'score', a.score, 'system_name', a.system_name)) as product_intents
          from (SELECT epk_id, explode(calibrated_score_array) AS (intent, score),
                CASE
                    WHEN intent = 'A0519.Operator' THEN 'A0519.Operator'
                    WHEN intent = 'A24.08.019.faq_pif' THEN 'A24.08.019.faq_pif'
                    WHEN intent = 'A24.08.021.faq_pension' THEN 'A24.08.021.faq_pension'
                    WHEN intent = 'A24.08.022.faq_ops' THEN 'A24.08.022.faq_ops'
                    WHEN intent = 'A24.08.023.faq_ipp' THEN 'A24.08.023.faq_ipp'
                    WHEN intent = 'A24.08.024.faq_kpp' THEN 'A24.08.024.faq_kpp'
                    WHEN intent = 'A24.08.030.faq_tax_return' THEN 'A24.08.030.faq_tax_return'
                    WHEN intent = 'A07.09708.About_bonuses' THEN 'About_bonuses'
                    WHEN intent = 'A05.06109.About_pos_loan' THEN 'About_pos_loan'
                    WHEN intent = 'A07.09711.About_tasks' THEN 'About_tasks'
                    WHEN intent = 'A0533.Annual_maintenance_fee' THEN 'Annual_maintenance_fee'
                    WHEN intent = 'A0534.Apply_for_a_new_card' THEN 'Apply_for_a_new_card'
                    WHEN intent = 'A0678.Apply_for_a_new_credit_card' THEN 'Apply_for_a_new_credit_card'
                    WHEN intent = 'A0677.Apply_for_a_new_debet_card' THEN 'Apply_for_a_new_debet_card'
                    WHEN intent = 'A07.01592.Authorization_in_the_SberSpasibo_app' THEN 'Authorization_in_the_SberSpasibo_app'
                    WHEN intent = 'A01.01662.Auto_repayment_by_credit_card' THEN 'Auto_repayment_by_credit_card'
                    WHEN intent = 'A31.12685.Bankruptcy' THEN 'Bankruptcy'
                    WHEN intent = 'A33.01649.Biometrics' THEN 'Biometrics'
                    WHEN intent = 'A05.01667.Deposit' THEN 'CB_Deposit'
                    WHEN intent = 'A05.06100.What_is_minimum_balance' THEN 'CB_WhatIsMinimumBalance'
                    WHEN intent = 'A07.01586.Can_not_use_SberSpasibo_bonuses' THEN 'Can_not_use_SberSpasibo_bonuses'
                    WHEN intent = 'A0532.Cancel_or_refund_payment' THEN 'Cancel_or_refund_payment'
                    WHEN intent = 'A02.01614.Car_loan_interest_rate' THEN 'Car_loan_interest_rate'
                    WHEN intent = 'A01.01583.Card_Mir' THEN 'Card_Mir'
                    WHEN intent = 'A0525.Cart_debet_reasons' THEN 'Card_debet_reasons'
                    WHEN intent = 'A25.01642.Card_for_selfemployed' THEN 'Card_for_selfemployed'
                    WHEN intent = 'A01.01690.Card_in_the_transport_stop_list' THEN 'Card_in_the_transport_stop_list'
                    WHEN intent = 'A01.01566.Card_replenishment' THEN 'Card_replenishment'
                    WHEN intent = 'A01.01572.Card_using_terms' THEN 'Card_using_terms'
                    WHEN intent = 'A01.01590.Cards_limits' THEN 'Cards_limits'
                    WHEN intent = 'A0546.Change_restore_login_or_password_SBOL' THEN 'Change_restore_login_or_password_SBOL'
                    WHEN intent = 'A07.01587.Changing_SberSpasibo_phone' THEN 'Changing_SberSpasibo_phone'
                    WHEN intent = 'A01.01560.Changing_card_details_during_reissue' THEN 'Changing_card_details_during_reissue'
                    WHEN intent = 'let_s_play' THEN 'Chislovoi_pazl_2048_top_briks'
                    WHEN intent = 'A0518.Close_card' THEN 'Close_card'
                    WHEN intent = 'A23.06623.Coins_made_of_precious_metals' THEN 'Coins_made_of_precious_metals'
                    WHEN intent = 'A0545.Comission_for_payments_and_transfers' THEN 'Comission_for_payments_and_transfers'
                    WHEN intent = 'A0541.Connect_notifications' THEN 'Connect_notifications'
                    WHEN intent = 'A02.01611.Consumer_loan_interest_rate' THEN 'Consumer_loan_interest_rate'
                    WHEN intent = 'A13.01648.Control_information' THEN 'Control_information'
                    WHEN intent = 'A07.09712.Coupons' THEN 'Coupons'
                    WHEN intent = 'A02.01571.Credit_application_status' THEN 'Credit_application_status'
                    WHEN intent = 'A01.01554.Credit_card_debt' THEN 'Credit_card_mapi'
                    WHEN intent = 'A02.01609.Credit_history' THEN 'Credit_history'
                    WHEN intent = 'A03.01661.Date_of_opening_of_the_card_account_and_deposit' THEN 'Date_of_opening_of_the_card_account_and_deposit'
                    WHEN intent = 'A05.01601.Deposit_tax' THEN 'Deposit_tax'
                    WHEN intent = 'A0540.Disable_mp_notifications' THEN 'Disable_mp_notifications'
                    WHEN intent = 'A13.01642.Disable_paid_services' THEN 'Disable_paid_services'
                    WHEN intent = 'A17.01650.Disable_subscriptions' THEN 'Disable_subscriptions'
                    WHEN intent = 'A04.01665.Do_not_pay_and_transfer' THEN 'Do_not_pay_and_transfer'
                    WHEN intent = 'A02.01657.Educational_loan_interest_rate' THEN 'Educational_loan_interest_rate'
                    WHEN intent = 'A23.01639.Exchange_of_coins_for_banknotes' THEN 'Exchange_of_coins_for_banknotes'
                    WHEN intent = 'A18.01555.Extend_safe_deposit_box_rental' THEN 'Extend_safe_deposit_box_rental'
                    WHEN intent = 'A13.01621.Find_info_about_SberID' THEN 'Find_info_about_SberID'
                    WHEN intent = 'A0528.Find_out_information_about_the_arrest' THEN 'Find_out_information_about_the_arrest'
                    WHEN intent = 'A0544.Find_out_payment_and_transfer_details' THEN 'Find_out_payment_and_transfer_details'
                    WHEN intent = 'A0538.Find_out_the_detail_of_card' THEN 'Find_out_the_detail_of_card'
                    WHEN intent = 'A18.01557.Find_out_the_status_of_the_safe' THEN 'Find_out_the_status_of_the_safe'
                    WHEN intent = 'A07.01582.Find_the_bonus_history' THEN 'Find_the_bonus_history'
                    WHEN intent = 'A0531.Funds_transfer_failed' THEN 'Funds_transfer_failed'
                    WHEN intent = 'A0548.Get_a_confirmation_of_the_operation' THEN 'Get_a_confirmation_of_the_operation'
                    WHEN intent = 'A03.01692.Get_a_loan_reference' THEN 'Get_a_loan_reference'
                    WHEN intent = 'A0536.Get_a_ready_card' THEN 'Get_a_ready_card'
                    WHEN intent = 'A0542.Get_a_statement_on_a_card' THEN 'Get_a_statement_on_a_card'
                    WHEN intent = 'A02.01584.Give_me_your_money' THEN 'Give_me_your_money'
                    WHEN intent = 'horoscope_for_today' THEN 'Goroskop_na_segodnia'
                    WHEN intent = 'A22.01622.Gosuslugi_account_confirmation' THEN 'Gosuslugi_account_confirmation'
                    WHEN intent = 'A07.09709.How_bonuses_are_awarded' THEN 'How_bonuses_are_awarded'
                    WHEN intent = 'A05.06102.How_calculate_rate_of_deposite' THEN 'How_calculate_rate_of_deposite'
                    WHEN intent = 'A01.01637.How_to_activate_the_card' THEN 'How_to_activate_the_card'
                    WHEN intent = 'A0526.How_to_block_a_card' THEN 'How_to_block_a_card'
                    WHEN intent = 'A04.01679.How_to_buy_a_subscription_for_transfers' THEN 'How_to_buy_a_subscription_for_transfers'
                    WHEN intent = 'A07.09638.How_to_buy_tickets' THEN 'How_to_buy_tickets'
                    WHEN intent = 'A0502.How_to_call_the_Bank' THEN 'How_to_call_the_Bank'
                    WHEN intent = 'A16.0506.How_to_change_DL' THEN 'How_to_change_DL'
                    WHEN intent = 'A16.0503.How_to_change_INILA' THEN 'How_to_change_INILA'
                    WHEN intent = 'A16.0507.How_to_change_STS' THEN 'How_to_change_STS'
                    WHEN intent = 'A16.0502.How_to_change_TIN' THEN 'How_to_change_TIN'
                    WHEN intent = 'A160509_How_to_change_agreements' THEN 'How_to_change_agreements'
                    WHEN intent = 'A01.01687.How_to_change_card_limits' THEN 'How_to_change_card_limits'
                    WHEN intent = 'A04.01598.How_to_change_daily_limit' THEN 'How_to_change_daily_limit'
                    WHEN intent = 'A160508_How_to_change_interests' THEN 'How_to_change_interests'
                    WHEN intent = 'A0527.How_to_change_pin_code' THEN 'How_to_change_pin_code'
                    WHEN intent = 'A13.01656.How_to_check_a_phone_number' THEN 'How_to_check_a_phone_number'
                    WHEN intent = 'A0501.How_to_check_your_SPASIBO_bonus_balance' THEN 'How_to_check_your_SPASIBO_bonus_balance'
                    WHEN intent = 'A0522.How_to_connect_bonuses_spasibo' THEN 'How_to_connect_bonuses_spasibo'
                    WHEN intent = 'A25.01641.How_to_create_check' THEN 'How_to_create_check'
                    WHEN intent = 'A01.01635.How_to_enable_contactless_payments' THEN 'How_to_enable_contactless_payments'
                    WHEN intent = 'A06.01500.How_to_enable_or_disable_autopayment' THEN 'How_to_enable_or_disable_autopayment'
                    WHEN intent = 'A04.01515.How_to_enable_or_disable_quick_payment_option' THEN 'How_to_enable_or_disable_quick_payment_option'
                    WHEN intent = 'A07.09643.How_to_exchange_bonuses_SberSpasibo' THEN 'How_to_exchange_bonuses_SberSpasibo'
                    WHEN intent = 'A13.01660.How_to_find_a_blocked_card' THEN 'How_to_find_a_blocked_card'
                    WHEN intent = 'A0506.How_to_find_out_the_cards_ready_status' THEN 'How_to_find_out_the_cards_ready_status'
                    WHEN intent = 'A0521.How_to_get_a_loan' THEN 'How_to_get_a_loan'
                    WHEN intent = 'A07.09710.How_to_get_more_bonuses' THEN 'How_to_get_more_bonuses'
                    WHEN intent = 'A07.09706.How_to_give_bonuses' THEN 'How_to_give_bonuses'
                    WHEN intent = 'A0512.How_to_increase_your_credit_limit' THEN 'How_to_increase_your_credit_limit'
                    WHEN intent = 'A13.01655.How_to_link_a_card_from_another_bank' THEN 'How_to_link_a_card_from_another_bank'
                    WHEN intent = 'A01.01634.How_to_make_priority_card' THEN 'How_to_make_priority_card'
                    WHEN intent = 'A01.04597.How_to_pay_QR' THEN 'How_to_pay_QR'
                    WHEN intent = 'A0524.How_to_reissue_a_card' THEN 'How_to_reissue_a_card'
                    WHEN intent = 'A13.01636.How_to_rename_card' THEN 'How_to_rename_card'
                    WHEN intent = 'A16.0512.How_to_see_receipts' THEN 'How_to_see_receipts'
                    WHEN intent = 'A01.01659.Info_for_government_employees' THEN 'Info_for_government_employees'
                    WHEN intent = 'A31.12686.Insurance_payments_on_deposits' THEN 'Insurance_payments_on_deposits'
                    WHEN intent = 'A17.01594.Job_search' THEN 'Job_search'
                    WHEN intent = 'A07.09713.Levels' THEN 'Levels'
                    WHEN intent = 'A0549.Loan_debts_were_not_written_off' THEN 'Loan_debts_were_not_written_off'
                    WHEN intent = 'A02.01567.Loan_payment_calculation' THEN 'Loan_payment_calculation'
                    WHEN intent = 'A01.01674.Make_a_salary_card' THEN 'Make_a_salary_card'
                    WHEN intent = 'A13.01663.Manage_applications' THEN 'Manage_applications'
                    WHEN intent = 'A09.01562.Mobile_operators_available_for_SMS_bank_connection' THEN 'Mobile_operators_available_for_SMS_bank_connection'
                    WHEN intent = 'A01.01619.Money_from_blocked_card' THEN 'Money_from_blocked_card'
                    WHEN intent = 'A01.01646.Multicurrency_card' THEN 'Multicurrency_card'
                    WHEN intent = 'A0523.No_credits_received_spasibo' THEN 'No_credits_received_spasibo'
                    WHEN intent = 'A31.12684.Nominal_account' THEN 'Nominal_account'
                    WHEN intent = 'A19.01578.Okko_subscription' THEN 'Okko_subscription'
                    WHEN intent = 'A18.01556.Open_the_safe' THEN 'Open_the_safe'
                    WHEN intent = 'сan_we_play' THEN 'Otgadai_personazha'
                    WHEN intent = 'A05.06108.POS_Post-service_installment' THEN 'POS_Post-service_installment'
                    WHEN intent = 'A30.01645.Payment_by_person' THEN 'Payment_by_person'
                    WHEN intent = 'A01.01561.Payments_abroad_and_special_mode' THEN 'Payments_abroad_and_special_mode'
                    WHEN intent = 'A0530.Personal_data_is_not_displayed' THEN 'Personal_data_is_not_displayed'
                    WHEN intent = 'A17.01576.Phone_loss' THEN 'Phone_loss'
                    WHEN intent = 'pop_it_antistress' THEN 'Pop_it_antistress_s_sharikom'
                    WHEN intent = 'A05.06107.Pos_purchase_return' THEN 'Pos_purchase_return'
                    WHEN intent = 'A31.12641.Power_of_attorney' THEN 'Power_of_attorney'
                    WHEN intent = 'A04.01589.Quick_payments_system' THEN 'Quick_payments_system'
                    WHEN intent = 'A02.01613.Refinancing_interest_rate' THEN 'Refinancing_interest_rate'
                    WHEN intent = 'A04.01653.Replenish_electronic_wallet' THEN 'Replenish_electronic_wallet'
                    WHEN intent = 'A07.09714.Routing_by_timing' THEN 'Routing_by_timing'
                    WHEN intent = 'A09.01565.SMS_bank_enabled_card_list' THEN 'SMS_bank_enabled_card_list'
                    WHEN intent = 'A09.01568.SMS_bank_price' THEN 'SMS_bank_price'
                    WHEN intent = 'A0543.SMS_not_coming' THEN 'SMS_not_coming'
                    WHEN intent = 'A17.01631.SberKids' THEN 'SberKids'
                    WHEN intent = 'A01.01580.SberPay' THEN 'SberPay'
                    WHEN intent = 'A07.01585.SberSpasibo_bonuses_loss' THEN 'SberSpasibo_bonuses_loss'
                    WHEN intent = 'A07.01581.SberSpasibo_bonuses_return' THEN 'SberSpasibo_bonuses_return'
                    WHEN intent = 'A07.01591.SberSpasibo_turn_off' THEN 'SberSpasibo_turn_off'
                    WHEN intent = 'A36.11202.SberCat' THEN 'Sber_Cat'
                    WHEN intent = 'A01.01666.Sbercard' THEN 'Sbercard'
                    WHEN intent = 'A48.01691.Sberdeal' THEN 'Sberdeal'
                    WHEN intent = 'A28.01620.Sbertips' THEN 'Sbertips'
                    WHEN intent IN ('W0130.Report_insured_event', 'W0072.Travel_insurance_info', 'W0050.My_insurance_products', 'W0142.Make_changes_to_policy', 'W0158.Property_insurance', 'W0115.OSAGO_promo_details', 'W0155.Credit_insurance', 'W0020.House_insurance_info', 'W0068.Card_protection_info', 'W0154.Cards_insurance', 'W0098.Tick_insurance_info', 'W0103.How_to_terminate_insurance_contract', 'W0129.Life_mortgage_insurance_info', 'W0120.Real_estate_mortgage_insurance_info', 'W0087.Real_estate_mortgage_insurance_buy_and_prolongation', 'W0092.Mortgage_insurance', 'W0049.CASCO_info', 'W0001.Nothing_found_response', 'W0071.Sports_protection_info', 'W0094.Get_copy_of_policy', 'W0101.Which_insurance_suitable_for_me', 'W0047.OSAGO_info', 'W0164.Loan_life_insurance_info', 'W0157.Life_insurance', 'W0089.My_statements_on_insurance_products', 'W0138.Auto_insurance', 'W0090.Real_estate_mortgage_insurance_prolongation', 'W0070.Injury_protection_info', 'W0088.Life_mortgage_insurance_buy_and_prolongation', 'W0178.Insurance', 'W0073.Insure_credit_card_holder') THEN 'SmartApp_UB'
                    WHEN intent = 'A0553.Something_about_a_bank' THEN 'Something_about_a_bank'
                    WHEN intent = 'A07.09640.Something_about_bonuses_SberSpasibo' THEN 'Something_about_bonuses_sberspasibo'
                    WHEN intent = 'A04.01675.Something_about_iban' THEN 'Something_about_iban'
                    WHEN intent = 'A17.01599.Something_about_limits' THEN 'Something_about_limits'
                    WHEN intent = 'A17.01606.Something_about_report' THEN 'Something_about_report'
                    WHEN intent = 'A17.01558.Status_of_the_request' THEN 'Status_of_the_request'
                    WHEN intent = 'absent' THEN 'Summa_chisel'
                    WHEN intent = 'A0547.Tariffs_and_fees_for_cash_deposit' THEN 'Tariffs_and_fees_for_cash_deposit'
                    WHEN intent = 'A25.01643.Tax_for_selfemployed' THEN 'Tax_for_selfemployed'
                    WHEN intent = 'A13.01600.Templates_and_my_operations' THEN 'Templates_and_my_operations'
                    WHEN intent = 'A07.09707.The_cost_of_the_bonus_program' THEN 'The_cost_of_the_bonus_program'
                    WHEN intent = 'A01.01564.Turn_off_paypass' THEN 'Turn_off_paypass'
                    WHEN intent = 'A0529.Unlock_card' THEN 'Unlock_card'
                    WHEN intent = 'А47.11203.Verification_of_operations_of_a_loved_one' THEN 'Verification_of_operations_of_a_loved_one'
                    WHEN intent = 'A0505.What_can_I_do_with_SPASIBO_bonuses' THEN 'What_can_I_do_with_SPASIBO_bonuses'
                    WHEN intent = 'A09.01569.What_is_SMS_bank' THEN 'What_is_SMS_bank'
                    WHEN intent = 'A01.01632.What_is_a_payment_account' THEN 'What_is_a_payment_account'
                    WHEN intent = 'A07.09715.What_is_one_bonus_equal_to' THEN 'What_is_one_bonus_equal_to'
                    WHEN intent = 'A05.06103.What_is_Setelem' THEN 'What_is_setelem'
                    WHEN intent = 'A0508.What_is_the_banks_operating_mode' THEN 'What_is_the_banks_operating_mode'
                    WHEN intent = 'A0517.What_should_i_do_if_my_card_expires' THEN 'What_should_i_do_if_my_card_expires'
                    WHEN intent = 'A0511.What_to_do_if_I_lost_my_card' THEN 'What_to_do_if_I_lost_my_card'
                    WHEN intent = 'A0520.What_to_do_with_fraud' THEN 'What_to_do_with_fraud'
                    WHEN intent = 'A05.06101.When_the_term_of_the_deposit_ends' THEN 'When_the_term_of_the_deposit_ends'
                    WHEN intent = 'A08.01633.Where_to_find_chat' THEN 'Where_to_find_chat'
                    WHEN intent = 'A0507.Where_to_view_the_history_of_operations' THEN 'Where_to_view_the_history_of_operations'
                    WHEN intent = 'A08.01658.Who_called_me' THEN 'Who_called_me'
                    WHEN intent = 'A0510.Why_credit_rejection' THEN 'Why_credit_rejection'
                    WHEN intent = 'A0516.Will_the_Bank_approve_the_loan' THEN 'Will_the_Bank_approve_the_loan'
                    WHEN intent = 'A25.01640.How_to_become_selfemployed' THEN 'become_selfemployed'
                    WHEN intent = 'bing' THEN 'bing'
                    WHEN intent = 'my_birthday' THEN 'birthday'
                    WHEN intent = 'brokerage_account' THEN 'brokerage_accounts'
                    WHEN intent = 'geo_route' THEN 'build_routes'
                    WHEN intent = 'buy_iis' THEN 'buy_iis'
                    WHEN intent = 'buy_pai_pif' THEN 'buy_pai_pif'
                    WHEN intent IN ('calculator', 'random_number') THEN 'calculator'
                    WHEN intent IN ('profile_app_address', 'A16.0501.How_to_change_email', 'A16.0505.How_to_change_address', 'profile_app_phone', 'A0509.How_to_change_your_phone_number') THEN 'canvas_address'
                    WHEN intent = 'carbon_footprint' THEN 'carbon_footprint'
                    WHEN intent = 'card_balance' THEN 'card_balance'
                    WHEN intent IN ('A01.01629.Card_for_kids', 'A01.01669.Kids_Sbercard') THEN 'cards_general_info'
                    WHEN intent IN ('catalog_chat_sbol_category', 'catalog_chat_sbol_resolve', 'rating_popup') THEN 'catalog'
                    WHEN intent = 'catalog_chat_b2c' THEN 'catalog_chat_b2c'
                    WHEN intent = 'A32.11642.Information_about_charity' THEN 'charity'
                    WHEN intent = 'A34.11646.Information_about_child_finance' THEN 'child_finance'
                    WHEN intent = 'COVID_Stats' THEN 'covid_stat'
                    WHEN intent = 'currency_converter' THEN 'currency_converter'
                    WHEN intent = 'A0677_e2e.Debit_card_opening_test' THEN 'debit_card_opening'
                    WHEN intent IN ('A05.01551.Deposit_selection', 'A05.01552.Deposit_open', 'A05.01604.Close_a_deposit', 'A05.01668.Deposit_for_kids', 'A0504.How_to_check_your_account_balance') THEN 'deposits_general_info'
                    WHEN intent = 'domclick' THEN 'domclick'
                    WHEN intent IN ('book_doctor', 'whats_with_my_booking', 'book_table', 'book_beauty') THEN 'duplex_app'
                    WHEN intent IN ('pfm_how_to_save_money', 'pfm_total_finance', 'pfm_my_income', 'subscription_feedback', 'pfm_my_subscriptions', 'pfm_disable_paid_services', 'pfm_my_expenses', 'pfm_my_budget', 'pfm_my_obligations', 'pfm_operations_history') THEN 'fin_advisor'
                    WHEN intent = 'fines_gibdd' THEN 'fines_gibdd'
                    WHEN intent = 'geo_currency' THEN 'geo_currency'
                    WHEN intent IN ('financial_problems_', 'restruct_problems_') THEN 'golosovoj_pomoshchni-71773-RIL'
                    WHEN intent IN ('gosuslugi_get_qr', 'gosuslugi_health', 'spravka_sudim', 'deti_obrazovanie', 'snils', 'A31.12647.Inheritance', 'etk_info', 'gosuslugi_pasport', 'zapis_doktor_free', 'suggestion_feedback_answer', 'zagran_plus_RF', 'covid_info', 'pushkin_card', 'bolche_uslug', 'razvod_obshiy', 'covid_vaccination', 'zagran_obshiy', 'gosuslugi_ndfl', 'common_passport', 'ils_info', 'zapis_detsad', 'kid_payments_info', 'fssp_info', 'brak_osnovnoy_infoquiz') THEN 'gosuslugi'
                    WHEN intent IN ('order_status', 'samokat') THEN 'grocery'
                    WHEN intent = 'how_to_invest' THEN 'how_to_invest'
                    WHEN intent = 'how_to_save_for_retirement' THEN 'how_to_save_for_retirement'
                    WHEN intent IN ('A02.01575.Reasons_for_loan_arrears', 'A05.06105.POS_arrange_problems', 'A02.01672.Change_the_credit_debiting_account', 'A01.02628.I_want_to_apply_for_a_consumer_loan', 'A02.01671.Change_the_loan_rate', 'A02.01612.Mortgage_interest_rate', 'A01.02626.I_want_to_get_a_car_loan', 'A0550.Credit_potential', 'A01.02625.I_want_to_apply_for_a_refinancing', 'A0537.Change_loan_terms', 'A02.01610.Interest_rate_on_the_loan', 'A02.01670.Change_the_date_of_debiting_the_loan', 'A02.01652.My_loans', 'A0539.How_to_pay_off_loan_debt', 'A02.01595.Educational_loan', 'A05.06106.Consultation_on_the_terms_of_POS', 'A01.02627.I_want_a_credit_for_goods', 'A02.01605.The_best_loan') THEN 'loans_general_info'
                    WHEN intent = 'market_index' THEN 'market_index'
                    WHEN intent = 'marketplace' THEN 'marketplace'
                    WHEN intent = 'me2me' THEN 'me2me'
                    WHEN intent = 'metal_exchange' THEN 'metal_exchange'
                    WHEN intent = 'mobile_payment' THEN 'mobile_payment'
                    WHEN intent IN ('radio', 'music') THEN 'music'
                    WHEN intent = 'geo_mycurrentgeo' THEN 'my_geo_position'
                    WHEN intent = 'nearest_sber' THEN 'nearest_sber'
                    WHEN intent = 'p2p' THEN 'p2p'
                    WHEN intent = 'p2p_partners' THEN 'p2p_partners'
                    WHEN intent = 'payment_app' THEN 'payment_app'
                    WHEN intent = 'A04.01607.Transfers_without_fee' THEN 'payments_and_transfers_general_info'
                    WHEN intent IN ('dsa_manager', 'personal_manager') THEN 'personal_manager'
                    WHEN intent = 'A02.01574.Personal_offer_checking' THEN 'personaldata_general_info'
                    WHEN intent = 'player_whats_playing' THEN 'player_whats_playing'
                    WHEN intent = 'promo_payments' THEN 'promo_payments'
                    WHEN intent = 'first_september_scenario' THEN 'quiz_smartvoice'
                    WHEN intent = 'refill_ipp' THEN 'refill_ipp'
                    WHEN intent = 'sbol_search' THEN 'sbol_search'
                    WHEN intent = 'share_price' THEN 'share_price'
                    WHEN intent = 'smartsearch' THEN 'smartsearch'
                    WHEN intent = 'A07.01593.SberSpasibo_bonus_categories' THEN 'spasibo_sberbank_bot'
                    WHEN intent IN ('sport_news', 'sport_olymp') THEN 'sport_skill'
                    WHEN intent = 'popcorn_order_search' THEN 'statusapp'
                    WHEN intent = 'the_transfer_of_a_pension' THEN 'the_transfer_of_a_pension'
                    WHEN intent IN ('current_time', 'get_date') THEN 'time'
                    WHEN intent = 'video' THEN 'video'
                    WHEN intent IN ('weather_range_date', 'weather_exact_location_date', 'weather_location_today_detailed', 'weather') THEN 'weather'
                    WHEN intent = 'whats_new' THEN 'whats_new'
                    WHEN intent IN ('cinema_tickets', 'geo_nearest_sber') THEN 'where_to_go_app'
                    ELSE '' END AS system_name FROM test_table) as a
        group by epk_id
          """).write.json("src/main/recources/result")

//    spark.sql(
//      """
//        select
//            transform(array_of_structs)
//        |""".stripMargin)

//    spark.sql(sqlQueryCheck).show()



//    //From Data (USING createDataFrame)
//    val dfFromData2 = spark.createDataFrame(data).toDF(columns:_*)
//    dfFromData2.show()
  }
}
