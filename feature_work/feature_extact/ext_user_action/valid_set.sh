awk -F \t  '(($(NF-1)==0 || $(NF-1)=="NULL") && $NF==1)||$NF==0||NR==1 {for(i=1;i<=NF;i++){if(i!=48){if(i!=NF){printf("%s\t",$i)}else{printf("%s",$i)}}}print "";}' user_features_with_reason_id > user_features_with_reason_id_48_cols
cp user_features_with_reason_id_48_cols user_features_test
da && cp ${features_lines}_rs_prob_with_reason_id.csv ./valid_set
cat ${features_lines}_rs_prob_with_reason_id.csv|grep mean_dis|grep out_range_ord_cnt_act_rate|grep -v prob  > ./valid_set/mean_dis_above_0.2.csv
cat ${features_lines}_rs_prob_with_reason_id.csv|egrep "act_cost_per_ord__(9.).*" > ./valid_set/high_act_cost.csv
cat ${features_lines}_rs_prob_with_reason_id.csv|egrep "day_total_order_num__1.8">  ./valid_set/day_total_order_num__above_1.8.csv
cat ${features_lines}_rs_prob_with_reason_id.csv|grep user_suggest_type|egrep "payed_arrived_rate__(0.1|0.2).*"  >  ./valid_set/payed_arrived_rate__below_0.3.csv
cat ${features_lines}_rs_prob_with_reason_id.csv|egrep "max_continue_order_num__(11|24).*" > ./valid_set/max_continue_order__above_11.csv
cat ${features_lines}_rs_prob_with_reason_id.csv|egrep "mean_in_poi_with_orderlist__0.(4|6)" > ./valid_set/mean_in_poi_with_orderlist__above_0.4.csv
cat ${features_lines}_rs_prob_with_reason_id.csv|egrep "fir_one_poi_rate__0.(8|9).*" >  ./valid_set/fir_one_rate__above_0.8.csv
cat ${features_lines}_rs_prob_with_reason_id.csv|egrep "7day14ord_days__(2|3|4|5|6|7|8).*"|grep -v "7day14ord_days__5.0" > ./valid_set/7day14ord_days__above_23.csv
da && cd valid_set && sh generate_random.sh


awk 'NR>2{for(i=1;i<=49;i++){if(i!=48){if(i!=49){printf("%s\t",$i)}else{printf("%s",$i)}}}print "";}' user_features_test