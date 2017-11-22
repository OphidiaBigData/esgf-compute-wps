import { Component, Input, OnInit } from '@angular/core';

import { DatasetCollection, Dataset } from './configure.service';

import * as d3 from 'd3';

class WorkflowModel { 
  selectedInput: Displayable;
  availableInputs: Displayable[] = [];
}

interface Displayable { 
  display(): string;
}

class DatasetWrapper implements Displayable {
  constructor(
    public dataset: string
  ) { }

  display() {
    return this.dataset;
  }
}

class Process implements Displayable {
  inputs: Displayable[];

  constructor(
    public id: string,
    public identifier: string,
    public x: number,
    public y: number
  ) { 
    this.inputs = [];
  }

  get uid() {
    return this.identifier + '-' + this.id;
  }

  display() {
    return this.uid;
  }
}

class Link {
  constructor(
    public src: Process,
    public dst: Process
  ) { }
}

@Component({
  selector: 'workflow',
  styles: [`
  .pane {
    padding: 1em;
  }

  .graph-container {
    width: 100%;
    min-height: calc(100vh - 100px);
  }

  .fill {
    min-height: calc(100vh - 100px);
  }

  svg {
    border: 1px solid #ddd;
  }
  `],
  templateUrl: './workflow.component.html'
})
export class WorkflowComponent implements OnInit{
  @Input() processes: string[];
  @Input() datasets: DatasetCollection;

  model: WorkflowModel = new WorkflowModel();

  drag: boolean;
  dragValue: string;

  nodes: Process[];
  links: Link[];
  selectedNode: Process;

  svg: any;
  svgLinks: any;
  svgNodes: any;

  ngOnInit() {
    this.nodes = [];

    this.links = [];

    this.svg = d3.select('svg')
      .on('mouseover', () => this.processMouseOver());

    this.svg.append('svg:defs')
      .append('svg:marker')
      .attr('id', 'end-arrow')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 60)
      .attr('refY', 0)
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('svg:path')
      .attr('d', 'M0,-5L10,0L0,5');

    let graph = this.svg.append('g')
      .classed('graph', true);

    this.svgLinks = graph.append('g')
      .classed('links', true);

    this.svgNodes = graph.append('g')
      .classed('nodes', true);
  }

  processMouseOver() {
    if (this.drag) {
      let origin = d3.mouse(d3.event.target);

      this.nodes.push(new Process(
        Math.random().toString(16).slice(2),
        this.dragValue,
        origin[0],
        origin[1]
      ));

      this.update();

      this.drag = false;
    }
  }

  addInput() {
    let index = this.model.availableInputs.indexOf(this.model.selectedInput);

    this.selectedNode.inputs.push(this.model.selectedInput);

    this.model.availableInputs.splice(index, 1);

    if (this.model.selectedInput instanceof Process) {
      this.links.push(new Link(<Process>this.model.selectedInput, this.selectedNode));

      this.update();
    }

    this.model.selectedInput = this.model.availableInputs[0];
  }

  removeInput(value: Process) {
    let index = this.selectedNode.inputs.indexOf(value);

    let removed = this.selectedNode.inputs.splice(index, 1);

    let size = this.model.availableInputs.length;

    this.model.availableInputs = this.model.availableInputs.concat(removed);

    if (size === 0) {
      this.model.selectedInput = this.model.availableInputs[0];
    }
  }

  dropped(event: any) {
    this.drag = true;
    this.dragValue = event.dragData;
  }

  clickNode() {
    this.selectedNode = <Process>d3.select(d3.event.target).datum();

    this.model.availableInputs = this.nodes.filter((value: Process) => {
      return this.selectedNode !== value && this.selectedNode.inputs.indexOf(value) < 0;
    });

    // remove the dst process from available inputs when selectedNode is the 
    // src of the link
    this.links.forEach((link: Link) => {
      if (this.selectedNode === link.src) {
        let index = this.model.availableInputs.indexOf(link.dst);

        if (index >= 0) {
          this.model.availableInputs.splice(index, 1);
        }
      }
    });

    // filter out the datasets already present in the input list
    // then wraper them in a displayable wrapper
    let datasets = Object.keys(this.datasets).filter((value: string) => {
      return this.selectedNode.inputs.findIndex((data: Displayable) => {
        return data.display() === value;
      }) < 0;
    }).map((value: string) => {
      return new DatasetWrapper(value);
    });

    this.model.availableInputs = this.model.availableInputs.concat(datasets);

    if (this.model.availableInputs.length > 0) {
      this.model.selectedInput = this.model.availableInputs[0];
    } else {
      // needs to be undefined to selected the default option
      this.model.selectedInput = undefined;
    }
  }

  update() {
    let links = this.svgLinks
      .selectAll('path')
      .attr('d', (d: any) => {
        return 'M' + d.src.x + ',' + d.src.y + 'L' + d.dst.x + ',' + d.dst.y;
      })
      .data(this.links);

    links.enter()
      .append('path')
      .style('stroke', 'black')
      .style('stroke-width', '2px')
      .attr('marker-end', 'url(#end-arrow)')
      .attr('d', (d: any) => {
        return 'M' + d.src.x + ',' + d.src.y + 'L' + d.dst.x + ',' + d.dst.y;
      });

    let nodes = this.svgNodes
      .selectAll('g')
      .attr('transform', (d: any) => { return `translate(${d.x}, ${d.y})`; })
      .data(this.nodes);

    let newNodes = nodes.enter()
      .append('g')
      .attr('data-toggle', 'modal')
      .attr('data-target', '#configure')
      .attr('transform', (d: any) => { return `translate(${d.x}, ${d.y})`; })
      .on('click', () => this.clickNode())
      .call(d3.drag()
        .on('drag', () => {
          let node = d3.event.subject;

          node.x += d3.event.dx;

          node.y += d3.event.dy;

          this.update();
        })
      );

    newNodes.append('circle')
      .attr('r', '60')
      .style('stroke', 'white')
      .style('stroke-width', '2px')
      .style('fill', '#ddd');

    newNodes.append('text')
      .attr('text-anchor', 'middle')
      .text((d: any) => { return d.identifier; });
  }
}
