package storm.trident;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.geom.Rectangle2D;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.JApplet;
import javax.swing.JFrame;
import javax.swing.border.Border;

import org.jgraph.JGraph;
import org.jgraph.graph.AttributeMap;
import org.jgraph.graph.DefaultEdge;
import org.jgraph.graph.DefaultGraphCell;
import org.jgraph.graph.GraphConstants;
import org.jgrapht.graph.DefaultDirectedGraph;

import backtype.storm.generated.Grouping;
import backtype.storm.generated.JavaObject;

import storm.trident.graph.GraphGrouper;
import storm.trident.graph.Group;
import storm.trident.planner.Node;
import storm.trident.planner.PartitionNode;
import storm.trident.planner.ProcessorNode;
import storm.trident.planner.SpoutNode;
import storm.trident.util.IndexedEdge;

public class TridentGraphDisplay extends JApplet {
	
	private static class NodeCellAdapter {
		private final Node _node;
		
		public NodeCellAdapter(Node node) {
			this._node = node;
		}
		
		@Override
		public String toString() {
			String s;
			if (_node instanceof SpoutNode) {
				s = getTitle((SpoutNode)_node);
			}
			else if (_node instanceof ProcessorNode){
				s = getTitle((ProcessorNode)_node);
			}
			else {
				assert _node instanceof PartitionNode;
				s = getTitle((PartitionNode)_node);
			}
			return s;
		}
		
		private static String getTitle(SpoutNode node) {
			return String.format("%s, %s%s", getOutFields(node), getParallelismHint(node), node.txId);
		}
		
		private static String getTitle(ProcessorNode node) {
			return String.format("%s, %s, %s%s", node.processor.getClass().getSimpleName(),
							getOutFields(node), getParallelismHint(node), (node.committer ? "comitter":""));
		}
		
		private static String getTitle(PartitionNode node) {
			return String.format("%s, %s", getGrouping(node.thriftGrouping), getParallelismHint(node));
		}
		
		private static String getGrouping(Grouping grouping) {
			String s = grouping.getSetField().toString();
			if (grouping.is_set_fields()) {
				int n = 0;
				s += " [";
				for(String f : grouping.get_fields()) {
					if (n > 0) { s += ", "; }
					s += f;
					n += 1;
				}
				s += "]";
			}
			if (grouping.is_set_custom_serialized()) {
				byte[] bytes = grouping.get_custom_serialized();
				ObjectInputStream oin;
				try {
					ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
					oin = new ObjectInputStream(byteStream);
					s += " " + oin.readObject().getClass().getSimpleName();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return s;
		}
		
		private static String getParallelismHint(Node node) {
			return node.parallelismHint == null ? "" : String.format("parallelismHint:%d, ", node.parallelismHint);
		}
		
		private static String getOutFields(Node node){
			int n = 0;
			String s = "[";
			for(String f : node.allOutputFields) {
				if (n > 0) { s += ", "; }
				s += f;
				n += 1;
			}
			s += "]";
			return s;
		}
	}
	
	private static class IndexedEdgeCellAdapter {
		private final IndexedEdge _edge;
		public IndexedEdgeCellAdapter(IndexedEdge edge) {
			this._edge = edge;
		}
		
		@Override
		public String toString() {
			return ((Node)_edge.source).streamId;
		}
	}
	
	private static class CellFactory implements JGraphModelAdapter.CellFactory<Node, IndexedEdge> {
		
		private GraphGrouper _nodeGrouper;
		private Map<Node, String> _spoutIds;
		private Map<Group, String> _boltIds;

		public CellFactory(GraphGrouper grouper, Map<Node, String> spoutIds, Map<Group, String> boltIds) {
			this._nodeGrouper = grouper;
			this._spoutIds = spoutIds;
			this._boltIds = boltIds;
		}

		@Override
		public DefaultEdge createEdgeCell(IndexedEdge jGraphTEdge) {
            return new DefaultEdge(new IndexedEdgeCellAdapter(jGraphTEdge));
		}

		@Override
		public DefaultGraphCell createVertexCell(Node jGraphTVertex) {
			DefaultGraphCell cell = new DefaultGraphCell(new NodeCellAdapter(jGraphTVertex));
			return cell;
		}
		
		private String getTitle(Node jGraphTVertex) {
			String title = "";
			String spoutId = this._spoutIds.get(jGraphTVertex);
			if (spoutId != null){
				title = spoutId;
			}
			else {
				Group group = this._nodeGrouper.nodeGroup(jGraphTVertex);
				if (group != null) {
					title = this._boltIds.get(group);
					assert title != null;
				}
			}
			return title;
		}
        
        public AttributeMap createVertexCellAttributes(Node jGraphTVertex) {
			String title = getTitle(jGraphTVertex);
			AttributeMap attrs = new AttributeMap();
			GraphConstants.setBackground(attrs, getNodeBackgroundColor(jGraphTVertex));
			Border border = BorderFactory.createLineBorder(Color.BLACK);
			GraphConstants.setBorder(attrs, BorderFactory.createTitledBorder(border, title));
			return attrs;
        }
        public AttributeMap createEdgeCellAttributes(IndexedEdge jGraphTEdge) {
        	return null;
        }
		
		private static Color getNodeBackgroundColor(Node node) {
			if (node instanceof SpoutNode) {
				return Color.decode("#9999FF");
			}
			else if (node instanceof ProcessorNode) {
				return ((ProcessorNode)node).committer ? Color.decode("#55bb55") : Color.decode("#99FF99");
			}
			else {
				assert node instanceof PartitionNode;
				return Color.decode("#C0C0C0");
			}
		}
		
	}
    
	private JGraphModelAdapter<Node, IndexedEdge> _jgraphAdapter;
	private DefaultDirectedGraph<Node, IndexedEdge> _graph;
	private GraphGrouper _nodeGrouper;
	private Map<Node, String> _spoutIds;
	private Map<Group, String> _boltIds;
	
	public static void displayGraph(DefaultDirectedGraph<Node, IndexedEdge> graph,
			GraphGrouper nodeGrouper, Map<Node, String> spoutIds, Map<Group, String> boltIds) {
		
		TridentGraphDisplay applet = new TridentGraphDisplay(graph, nodeGrouper, spoutIds, boltIds);
		
		applet.init();
		
        JFrame frame = new JFrame();
        frame.getContentPane().add(applet);
        frame.setTitle("Trident Planner Graph");
        frame.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
        while(frame.isVisible()){
        	sleepSeconds(1);
        }
    }
	
	private static void sleepSeconds(int seconds){
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
		}
	}
	
	public TridentGraphDisplay(DefaultDirectedGraph<Node, IndexedEdge> graph,
			GraphGrouper nodeGrouper, Map<Node, String> spoutIds, Map<Group, String> boltIds) {
		this._graph = graph;
		this._nodeGrouper = nodeGrouper;
		this._spoutIds = spoutIds;
		this._boltIds = boltIds;
	}

	@Override
	public void init(){
		AttributeMap defaultVertexAttrs = createDefaultVertexAttributes();
		AttributeMap defaultEdgeAttrs = createDefaultEdgeAttributes();
		JGraphModelAdapter.CellFactory<Node, IndexedEdge> factory = new CellFactory(_nodeGrouper, _spoutIds, _boltIds);
		this._jgraphAdapter = new JGraphModelAdapter<Node, IndexedEdge>(this._graph, defaultVertexAttrs, defaultEdgeAttrs, factory);
		JGraph jgraph = new JGraph(this._jgraphAdapter);
        adjustDisplaySettings(jgraph);
        getContentPane().add(jgraph);
        resize(DEFAULT_SIZE);
	}
	
    private static final Color DEFAULT_BG_COLOR = Color.decode("#FAFBFF");
    private static final Dimension DEFAULT_SIZE = new Dimension(800, 600);
    
    private AttributeMap createDefaultVertexAttributes() {
		AttributeMap attrs = JGraphModelAdapter.createDefaultVertexAttributes();
		GraphConstants.setAutoSize(attrs, true);
		GraphConstants.setForeground(attrs, Color.black);
        GraphConstants.setBorder(attrs, BorderFactory.createLineBorder(Color.black, 1));
		return attrs;
    }
    
    private AttributeMap createDefaultEdgeAttributes() {
		AttributeMap attrs = JGraphModelAdapter.createDefaultEdgeAttributes(this._graph);
		return attrs;
    }
	
	private void adjustDisplaySettings(JGraph jg)
    {
        jg.setPreferredSize(DEFAULT_SIZE);

        Color c = DEFAULT_BG_COLOR;
        String colorStr = null;

        try {
            colorStr = getParameter("bgcolor");	
        } catch (Exception e) {
        }

        if (colorStr != null) {
            c = Color.decode(colorStr);
        }

        jg.setBackground(c);
    }


    @SuppressWarnings("unchecked")
    private static void positionVertexAt(JGraphModelAdapter<Node, IndexedEdge> jgraphAdapter, Object vertex, int x, int y)
    {
        DefaultGraphCell cell = jgraphAdapter.getVertexCell(vertex);
        AttributeMap attr = cell.getAttributes();
        Rectangle2D bounds = GraphConstants.getBounds(attr);

        Rectangle2D newBounds =
            new Rectangle2D.Double(
                x,
                y,
                bounds.getWidth(),
                bounds.getHeight());

        GraphConstants.setBounds(attr, newBounds);

        // TODO: Clean up generics once JGraph goes generic
        AttributeMap cellAttr = new AttributeMap();
        cellAttr.put(cell, attr);
        jgraphAdapter.edit(cellAttr, null, null, null);
    }
}
