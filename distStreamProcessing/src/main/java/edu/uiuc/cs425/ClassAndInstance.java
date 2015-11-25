package edu.uiuc.cs425;

public class ClassAndInstance {
	
	private Class<?>		m_cClass;
	private Object		m_oClassObject;
	
	
	public Object getM_oClassObject() {
		return m_oClassObject;
	}
	public void setM_oClassObject(Object m_oClassObject) {
		this.m_oClassObject = m_oClassObject;
	}
	public Class<?> getM_cClass() {
		return m_cClass;
	}
	public void setM_cClass(Class<?> m_cClass) {
		this.m_cClass = m_cClass;
	}
	
}
