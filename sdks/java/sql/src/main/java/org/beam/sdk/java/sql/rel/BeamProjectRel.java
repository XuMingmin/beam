package org.beam.sdk.java.sql.rel;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.beam.sdk.java.sql.interpreter.BeamSQLExpressionExecutor;
import org.beam.sdk.java.sql.interpreter.BeamSQLSpELExecutor;
import org.beam.sdk.java.sql.planner.BeamPipelineCreator;
import org.beam.sdk.java.sql.planner.BeamSQLRelUtils;
import org.beam.sdk.java.sql.schema.BeamSQLRecordType;
import org.beam.sdk.java.sql.schema.BeamSQLRow;
import org.beam.sdk.java.sql.transform.BeamSQLProjectFn;

public class BeamProjectRel extends Project implements BeamRelNode {

  /**
   * projects: {@link RexLiteral}, {@link RexInputRef}, {@link RexCall}
   * 
   * @param cluster
   * @param traits
   * @param input
   * @param projects
   * @param rowType
   */
  public BeamProjectRel(RelOptCluster cluster, RelTraitSet traits, RelNode input,
      List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traits, input, projects, rowType);
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects,
      RelDataType rowType) {
    return new BeamProjectRel(getCluster(), traitSet, input, projects, rowType);
  }

  @Override
  public void buildBeamPipeline(BeamPipelineCreator planCreator) throws Exception {
    RelNode input = getInput();
    BeamSQLRelUtils.getBeamRelInput(input).buildBeamPipeline(planCreator);

    String stageName = BeamSQLRelUtils.getStageName(this);

    PCollection<BeamSQLRow> upstream = planCreator.getLatestStream();

    BeamSQLExpressionExecutor executor = new BeamSQLSpELExecutor(this);
    
    PCollection<BeamSQLRow> projectStream = upstream.apply(stageName,
        ParDo.of(new BeamSQLProjectFn(getRelTypeName(), executor, BeamSQLRecordType.from(rowType))));

    planCreator.setLatestStream(projectStream);

    System.out.println("Build: apply_project " + stageName);
    
  }

}
