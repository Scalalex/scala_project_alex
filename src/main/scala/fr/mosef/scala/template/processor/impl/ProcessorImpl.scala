package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.DataFrame

class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame): DataFrame = {
    // Rapport 1: Nombre de lignes par groupe
    val report1 = inputDF.groupBy("group_key").count()

    // Rapport 2: Somme des valeurs de "field1" par groupe
    val report2 = inputDF.groupBy("group_key").sum("field1")

    // Rapport 3: Autre transformation ou traitement de données

    // Vous pouvez retourner l'un des rapports ou les combiner selon vos besoins
    // Par exemple, ici je retourne seulement le rapport 1
    report1
  }

}
