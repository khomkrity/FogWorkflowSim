package org.fog.test.perfeval;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.event.*;
import java.lang.ModuleLayer.Controller;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import javax.swing.*;
import javax.swing.event.*;

import com.mindfusion.charting.*;
import com.mindfusion.charting.animation.*;
import com.mindfusion.charting.components.LayoutAlignment;
import com.mindfusion.charting.swing.*;
import com.mindfusion.common.ObservableList;
import com.mindfusion.drawing.*;

public class ChartUI extends JFrame {
	private BarChart barChart;
	private String offloadingStrategy;
	private List<Double[]> results;
	private List<String> algorithmNames;
	private Map<String, Integer> servers;
	private double inputDelay;

	/**
	 * Construct ChartUI class with given resource usage.
	 * @param offloadingStrategy resource allocation strategy i.e., none, simple, all-in-fog, and all-in-cloud.
	 * @param algorithmNames name of all the task-to-VM allocation algorithms.
	 * @param results list of arrays of Double containing time, energy, cost, and total delay.
	 * @param servers map between server name and its amount.
	 * @param inputDelay additional input constraint i.e., delay. 
	 */
	ChartUI(String offloadingStrategy, List<String> algorithmNames, List<Double[]> results,
			Map<String, Integer> servers, double inputDelay) {
		this.offloadingStrategy = offloadingStrategy;
		this.results = results;
		this.algorithmNames = algorithmNames;
		this.servers = servers;
		this.inputDelay = inputDelay;
	}

	private void initChart() {
		barChart = new BarChart();

		barChart.setSeries(createSeries());

		// fill all elements of a series using a common brush
		barChart.getTheme().setCommonSeriesFills(
				Arrays.asList(new SolidBrush(new Color(102, 154, 204)), new SolidBrush(new Color(240, 212, 50)),
						new SolidBrush(new Color(206, 0, 0)), new SolidBrush(new Color(97, 201, 44))));

		barChart.getTheme().setCommonSeriesStrokes(Arrays.asList(new SolidBrush(new Color(0, 52, 102))));
		barChart.getTheme().setCommonSeriesStrokeThicknesses(Arrays.asList(0.1));
		barChart.getTheme().setDataLabelsBrush(new SolidBrush(new Color(0, 52, 102)));
		barChart.getTheme().setDataLabelsFontSize(15);
		barChart.getTheme().setDataLabelsFontStyle(EnumSet.of(FontStyle.BOLD));
		barChart.getTheme().setLegendTitleFontSize(12);
		barChart.getTheme().setGridColor1(Color.white);
		barChart.getTheme().setGridColor2(new Color(240, 240, 240));
		barChart.getTheme().setGridLineColor(new Color(192, 192, 192));
		barChart.getTheme().setAxisTitleFontSize(12);
		barChart.getTheme().setAxisLabelsFontSize(14);

		// set appearance properties
		barChart.setBarSpacingRatio(2);
		barChart.setShowHighlight(false);
		barChart.getXAxis().setInterval(1.0);
		barChart.setShowLegend(true);
		barChart.setLegendHorizontalAlignment(LayoutAlignment.Near);
		barChart.setLegendTitle("Legend");
		barChart.getXAxis().setTitle("Algorithms");
		barChart.getYAxis().setTitle("Resource Usage");
		barChart.setGridType(GridType.Horizontal);

		// animate the bars
		Animation animation = new Animation(AnimationSpeed.SpeedUp);
		AnimationTimeline timeline = new AnimationTimeline();
		timeline.addAnimation(AnimationType.PerElementAnimation, 2.0f, (Renderer2D) barChart.getSeriesRenderer());
		animation.addTimeline(timeline);
		animation.runAnimation();
	}

	ObservableList<Series> createSeries() {
		List<String> algorithms = algorithmNames;
		List<Double> times = new ArrayList<>();
		List<Double> energies = new ArrayList<>();
		List<Double> costs = new ArrayList<>();
		List<Double> delays = new ArrayList<>();
		List<String> timeTopLabels = new ArrayList<>();
		List<String> energyTopLabels = new ArrayList<>();
		List<String> costTopLabels = new ArrayList<>();
		List<String> delayTopLabels = new ArrayList<>();
		int precision = 4;

		for (Double[] records : results) {
			Double time = records[1];
			Double energy = records[2];
			Double cost = records[3];
			Double delay = records[4];
			times.add(time);
			energies.add(energy);
			costs.add(cost);
			delays.add(delay);
			timeTopLabels.add(getTopLabel(time, precision));
			energyTopLabels.add(getTopLabel(energy, precision));
			costTopLabels.add(getTopLabel(cost, precision));
			delayTopLabels.add(getTopLabel(delay, 2));
		}

		BarSeries timeSeries = new BarSeries(times, null, timeTopLabels);
		timeSeries.setXAxisLabels(algorithms);
		timeSeries.setTitle("Time");

		BarSeries energySeries = new BarSeries(energies, null, energyTopLabels);
		energySeries.setTitle("Energy");

		BarSeries costSeries = new BarSeries(costs, null, costTopLabels);
		costSeries.setTitle("Cost");

		BarSeries delaySeries = new BarSeries(delays, null, delayTopLabels);
		delaySeries.setTitle("Delay");

		return new ObservableList<Series>(Arrays.asList(timeSeries, energySeries, costSeries, delaySeries));
	}

	/**
	 * Open new Chart UI window to visualize the resource usage
	 *
	 */
	void initFrame() {
		setTitle("Bar Chart");
		setSize(800, 600);
		setExtendedState(java.awt.Frame.MAXIMIZED_BOTH);
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		initChart();

		JPanel controls = new JPanel();
		controls.setLayout(null);
		controls.setPreferredSize(new Dimension(1000, 200));

		JCheckBox xTicks = new JCheckBox("Show X Ticks");
		xTicks.setSelected(barChart.getShowXTicks());
		xTicks.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				barChart.setShowXTicks(xTicks.isSelected());
			}
		});
		addComponent(controls, xTicks);

		JCheckBox yTicks = new JCheckBox("Show Y Ticks");
		yTicks.setSelected(barChart.getShowYTicks());
		yTicks.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				barChart.setShowYTicks(yTicks.isSelected());
			}
		});
		addComponent(controls, yTicks);

		JCheckBox xCoords = new JCheckBox("Show X Coordinates");
		xCoords.setSelected(barChart.getShowXCoordinates());
		xCoords.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				barChart.setShowXCoordinates(xCoords.isSelected());
			}
		});
		addComponent(controls, xCoords);

		JCheckBox yCoords = new JCheckBox("Show Y Coordinates");
		yCoords.setSelected(barChart.getShowYCoordinates());
		yCoords.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				barChart.setShowYCoordinates(yCoords.isSelected());
			}
		});
		addComponent(controls, yCoords);

		newCol();

		// addComponent(controls, combine("BarLayout ", new LayoutComboBox()));
		addComponent(controls, combine("Grid Type   ", new GridComboBox()));

		JCheckBox labelsCheckbox = new JCheckBox("Show Data Labels");
		labelsCheckbox.setSelected(true);
		labelsCheckbox.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				if (labelsCheckbox.isSelected())
					barChart.setShowDataLabels(EnumSet.allOf(LabelKinds.class));
				else
					barChart.setShowDataLabels(EnumSet.noneOf(LabelKinds.class));
				barChart.repaint();
			}
		});
		addComponent(controls, labelsCheckbox);

		JCheckBox horizontalCheckbox = new JCheckBox("Horizontal bars");
		horizontalCheckbox.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				barChart.setHorizontalBars(horizontalCheckbox.isSelected());
			}
		});
		addComponent(controls, horizontalCheckbox);

		newCol();

		xMin = new MinSlider(true);
		yMin = new MinSlider(false);

		xMax = new MaxSlider(true, 6);
		xMax.setMaximum(6);
		yMax = new MaxSlider(false, 100);

		// addComponent(controls, combine("XAxis Min", xMin));
		// addComponent(controls, combine("YAxis Min", yMin));

		JSlider spacingSlider = new JSlider(1, 10);
		spacingSlider.setValue(1);
		spacingSlider.addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent e) {
				barChart.setBarSpacingRatio((double) spacingSlider.getValue());
			}
		});
		addComponent(controls, combine("Space Ratio", spacingSlider));

		newCol();

		addComponent(controls, combine("XAxis Max", xMax));
		addComponent(controls, combine("YAxis Max", yMax));

		JSlider originSlider = new JSlider(0, 30);
		originSlider.setValue(0);
		originSlider.addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent e) {
				barChart.getYAxis().setOrigin((double) originSlider.getValue());
			}
		});

		newCol();
		addComponent(controls, new JLabel("Environment Settings"));
		addComponent(controls, new JLabel("Offloading Strategy: " + offloadingStrategy));
		addComponent(controls, new JLabel("Cloud Server: " + servers.get("cloud")));
		addComponent(controls, new JLabel("Fog Node: " + servers.get("fog")));
		addComponent(controls, new JLabel("End Device: " + servers.get("mobile")));
		addComponent(controls, new JLabel("Input Delay: " + inputDelay));
		// addComponent(controls, combine("Y origin", originSlider));

		getContentPane().setLayout(new BorderLayout());
		getContentPane().add(controls, BorderLayout.SOUTH);
		getContentPane().add(barChart, BorderLayout.CENTER);
		setVisible(true);
	}

//	static public void main(String[] args) {
//		ChartUI frame = new ChartUI();
//		frame.initFrame();
//	}

	class GridComboBox extends JComboBox<String> implements ActionListener {
		public GridComboBox() {
			super(new String[] { "Crossed", "Horizontal", "None", "Vertical" });
			this.setSelectedIndex(2);
			this.setMaximumSize(new Dimension(100, 30));
			addActionListener(this);
		}

		@Override
		public void actionPerformed(ActionEvent e) {
			barChart.setGridType(GridType.valueOf(getSelectedItem().toString()));
		}

		static private final long serialVersionUID = 1L;
	}

	class LayoutComboBox extends JComboBox<String> implements ActionListener {
		public LayoutComboBox() {
			super(new String[] { "Overlay", "Side By Side", "Stack" });
			this.setSelectedIndex(1);

			this.setMaximumSize(new Dimension(100, 30));
			addActionListener(this);
		}

		@Override
		public void actionPerformed(ActionEvent e) {
			int index = getSelectedIndex();
			switch (index) {
			case 0:
				barChart.setBarLayout(BarLayout.Overlay);
				break;
			case 1:
				barChart.setBarLayout(BarLayout.SideBySide);
				break;
			case 2:
				barChart.setBarLayout(BarLayout.Stack);
				break;
			}
		}

		static private final long serialVersionUID = 1L;
	}

	class MinSlider extends JSlider implements ChangeListener {
		private boolean xAxis;

		public MinSlider(boolean xAxis) {
			setMinimum(-1);
			setMaximum(11);
			setValue(0);
			addChangeListener(this);
			this.xAxis = xAxis;
		}

		@Override
		public void stateChanged(ChangeEvent event) {
			if (xAxis) {
				double xMin = (double) getValue();
				xMin = Math.min(xMin, xMax.getValue() - 1);
				barChart.getXAxis().setMinValue(xMin);
			} else {
				double yMin = (double) getValue();
				yMin = Math.min(yMin, yMax.getValue() - 1);
				barChart.getYAxis().setMinValue(yMin);
			}
		}

		static private final long serialVersionUID = 1L;
	}

	private class MaxSlider extends JSlider implements ChangeListener {
		private boolean xAxis;

		public MaxSlider(boolean xAxis, int initialValue) {
			setMaximum(1000);
			setValue(initialValue);
			addChangeListener(this);
			this.xAxis = xAxis;
		}

		@Override
		public void stateChanged(ChangeEvent arg0) {
			if (xAxis) {
				double xMax = (double) getValue();
				if (xMax > 7)
					xMax = 7;
				xMax = Math.max(xMax, xMin.getValue() + 1);
				barChart.getXAxis().setMaxValue(xMax);
			} else {
				double yMax = (double) getValue();
				yMax = Math.max(yMax, yMin.getValue() + 1);
				barChart.getYAxis().setMaxValue(yMax);
			}
		}

		static private final long serialVersionUID = 1L;
	}

	private static JPanel combine(String text, Container element) {
		JPanel out = new JPanel();
		out.setLayout(new BoxLayout(out, BoxLayout.X_AXIS));
		out.add(new JLabel(text));
		out.add(element);
		return out;
	}

	JPanel addComponent(JPanel panel, Container element) {
		yStart += 25;
		if (element != null) {
			element.setBounds(xStart, yStart, 250, 20);
			panel.add(element);
		}
		return panel;
	}

	void newCol() {
		xStart += 300;
		yStart = 0;
	}

	private int xStart = 0;
	private int yStart = 0;

	private MaxSlider xMax;
	private MaxSlider yMax;

	private MinSlider xMin;
	private MinSlider yMin;

	static private final long serialVersionUID = 1L;

	private static String getTopLabel(Double resource, int precision) {
		return BigDecimal.valueOf(resource).setScale(precision, RoundingMode.HALF_UP).toString();
	}

}