import React from 'react';
import styled from '@emotion/styled';
import {Observer} from 'mobx-react';
import PropTypes from 'prop-types';

import Alert from 'app/components/alert';
import Button from 'app/components/button';
import ButtonBar from 'app/components/buttonBar';
import PanelAlert from 'app/components/panels/panelAlert';
import {t} from 'app/locale';
import space from 'app/styles/space';
import {defined} from 'app/utils';
import {sanitizeQuerySelector} from 'app/utils/sanitizeQuerySelector';
import Field from 'app/views/settings/components/forms/field';
import FieldControl from 'app/views/settings/components/forms/field/fieldControl';
import FieldErrorReason from 'app/views/settings/components/forms/field/fieldErrorReason';
import FormFieldControlState from 'app/views/settings/components/forms/formField/controlState';
import FormModel, {MockModel} from 'app/views/settings/components/forms/model';
import ReturnButton from 'app/views/settings/components/forms/returnButton';

import {FieldValue} from '../type';

/**
 * Some fields don't need to implement their own onChange handlers, in
 * which case we will receive an Event, but if they do we should handle
 * the case where they return a value as the first argument.
 */
const getValueFromEvent = (valueOrEvent?: FieldValue | MouseEvent, e?: MouseEvent) => {
  const event = e || valueOrEvent;
  const value = defined(e) ? valueOrEvent : event && event.target && event.target.value;

  return {value, event};
};

/**
 * This is a list of field properties that can accept a function taking the
 * form model, that will be called to determine the value of the prop upon an
 * observed change in the model.
 */
const propsToObserver = ['help', 'inline', 'highlighted', 'visible', 'disabled'] as const;

//functions that get evaluated in observedProps
type ObserverReducerFn<T> = (props: Props & {model: FormModel}) => T;
type ObserverOrValue<T> = T | ObserverReducerFn<T>;

//
type ObservableProps = {
  [T in typeof propsToObserver[number]]?: ObserverOrValue<Field['props'][T]>;
};

type ResolvedObservableProps = {
  [T in typeof propsToObserver[number]]?: Field['props'][T];
};

type BaseProps = {
  /**
   * Name of the field
   */
  name: string;
  /**
   * Used to render the actual control
   */
  children: (renderProps) => React.ReactNode;
  /**
   * Extra styles to apply to the field
   */
  style?: React.CSSProperties;
  /**
   * When the field is blurred should it automatically persist it's value into
   * the model
   */
  saveOnBlur?: boolean;
  /**
   * The message to display when saveOnBlur is false
   */
  saveMessage?:
    | React.ReactNode
    | ((props: ResolvedProps & {value: FieldValue}) => React.ReactNode);
  /**
   * The alert type to use when saveOnBlur is false
   */
  saveMessageAlertType?: React.ComponentProps<typeof Alert>['type'];
  onKeyDown?: (value, event) => void;
  onBlur?: (value, event) => void;
  onChange?: (value, event) => void;
  hideErrorMessage?: boolean;
  selectionInfoFunction?: (props) => null | React.ReactNode;
  placeholder?: ObserverOrValue<React.ReactNode>; // TODO: What's using this?
  formatMessageValue?: boolean | Function; //used in prettyFormString
  defaultValue?: any; //TODO(TS): Do we need this?
  resetOnError?: boolean;
  /**
   * Tranform input when a value is set to the model.
   */
  transformInput?: (value: any) => any;
  /**
   * Transform data when saving on blur.
   */
  getData?: (value: any) => any;
};

/**
 * ResolvedProps do NOT include props which may be given functions that are
 * reacted on. Resolved props are used inside of makeField.
 */
type ResolvedProps = BaseProps & Field['props'];

type Props = BaseProps &
  ObservableProps &
  Omit<Field['props'], typeof propsToObserver[number]>;

class FormField extends React.Component<Props> {
  static propTypes = {
    name: PropTypes.string.isRequired,

    /**
     * Iff false, disable saveOnBlur for field, instead show a save/cancel button
     */
    saveOnBlur: PropTypes.bool,

    /**
     * If saveOnBlur is false, then an optional saveMessage can be used to let
     * the user know what's going to happen when they save a field.
     */
    saveMessage: PropTypes.oneOfType([PropTypes.node, PropTypes.func]),

    /**
     * The "alert type" to use for the save message.
     * Probably only "info"/"warning" should be used.
     */
    saveMessageAlertType: PropTypes.oneOf(['', 'info', 'warning', 'success', 'error']),

    /**
     * A function producing an optional component with extra information.
     */
    selectionInfoFunction: PropTypes.func,

    /**
     * Should hide error message?
     */
    hideErrorMessage: PropTypes.bool,

    // Default value to use for form field if value is not specified in `<Form>` parent
    defaultValue: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.number,
      PropTypes.func,
    ]),

    // the following should only be used without form context
    onChange: PropTypes.func,
    onBlur: PropTypes.func,
    onKeyDown: PropTypes.func,
    onMouseOver: PropTypes.func,
    onMouseOut: PropTypes.func,
  };

  static contextTypes = {
    location: PropTypes.object,
    form: PropTypes.object,
  };

  static defaultProps = {
    hideErrorMessage: false,
    flexibleControlStateSize: false,
  };

  componentDidMount() {
    // Tell model about this field's props
    this.getModel().setFieldDescriptor(this.props.name, this.props);
  }

  componentWillUnmount() {
    this.getModel().removeField(this.props.name);
  }

  input?: React.Ref<HTMLElement>;

  getError() {
    return this.getModel().getError(this.props.name);
  }

  getId() {
    return sanitizeQuerySelector(this.props.name);
  }

  getModel() {
    return this.context.form !== undefined
      ? this.context.form
      : new MockModel(this.props);
  }

  // Only works for styled inputs
  // Attempts to autofocus input field if field's name is in url hash
  handleInputMount = ref => {
    if (ref && !this.input) {
      const hash = this.context.location && this.context.location.hash;

      if (!hash) {
        return;
      }

      if (hash !== `#${this.props.name}`) {
        return;
      }

      // Not all form fields have this (e.g. Select fields)
      if (typeof ref.focus === 'function') {
        ref.focus();
      }
    }

    this.input = ref;
  };

  /**
   * Update field value in form model
   */
  handleChange = (...args) => {
    const {name, onChange} = this.props;
    const {value, event} = getValueFromEvent(...args);
    const model = this.getModel();

    if (onChange) {
      onChange(value, event);
    }

    model.setValue(name, value);
  };

  /**
   * Notify model of a field being blurred
   */
  handleBlur = (...args) => {
    const {name, onBlur} = this.props;
    const {value, event} = getValueFromEvent(...args);
    const model = this.getModel();

    if (onBlur) {
      onBlur(value, event);
    }

    // Always call this, so model can decide what to do
    model.handleBlurField(name, value);
  };

  /**
   * Handle keydown to trigger a save on Enter
   */
  handleKeyDown = (...args) => {
    const {onKeyDown, name} = this.props;
    const {value, event} = getValueFromEvent(...args);
    const model = this.getModel();

    if (event.key === 'Enter') {
      model.handleBlurField(name, value);
    }

    if (onKeyDown) {
      onKeyDown(value, event);
    }
  };

  /**
   * Handle saving an individual field via UI button
   */
  handleSaveField = () => {
    const {name} = this.props;
    const model = this.getModel();

    model.handleSaveField(name, model.getValue(name));
  };

  handleCancelField = () => {
    const {name} = this.props;
    const model = this.getModel();

    model.handleCancelSaveField(name);
  };

  render() {
    const {
      className,
      name,
      hideErrorMessage,
      flexibleControlStateSize,
      saveOnBlur,
      saveMessage,
      saveMessageAlertType,
      selectionInfoFunction,
      hideControlState,

      // Don't pass `defaultValue` down to input fields, will be handled in form model
      defaultValue: _defaultValue,
      ...otherProps
    } = this.props;
    const id = this.getId();
    const model = this.getModel();
    const saveOnBlurFieldOverride = typeof saveOnBlur !== 'undefined' && !saveOnBlur;

    const makeField = (resolvedObservedProps?: ResolvedObservableProps) => {
      const props = {...otherProps, ...resolvedObservedProps} as ResolvedProps;

      return (
        <React.Fragment>
          <Field
            id={id}
            className={className}
            flexibleControlStateSize={flexibleControlStateSize}
            {...props}
          >
            {({alignRight, inline, disabled, disabledReason}) => (
              <FieldControl
                disabled={disabled}
                disabledReason={disabledReason}
                inline={inline}
                alignRight={alignRight}
                flexibleControlStateSize={flexibleControlStateSize}
                hideControlState={hideControlState}
                controlState={<FormFieldControlState model={model} name={name} />}
                errorState={
                  <Observer>
                    {() => {
                      const error = this.getError();
                      const shouldShowErrorMessage = error && !hideErrorMessage;
                      if (!shouldShowErrorMessage) {
                        return null;
                      }
                      return <FieldErrorReason>{error}</FieldErrorReason>;
                    }}
                  </Observer>
                }
              >
                <Observer>
                  {() => {
                    const error = this.getError();
                    const value = model.getValue(name);
                    const showReturnButton = model.getFieldState(
                      name,
                      'showReturnButton'
                    );

                    return (
                      <React.Fragment>
                        {this.props.children({
                          ref: this.handleInputMount,
                          ...props,
                          model,
                          name,
                          id,
                          onKeyDown: this.handleKeyDown,
                          onChange: this.handleChange,
                          onBlur: this.handleBlur,
                          // Fixes react warnings about input switching from controlled to uncontrolled
                          // So force to empty string for null values
                          value: value === null ? '' : value,
                          error,
                          disabled,
                          initialData: model.initialData,
                        })}
                        {showReturnButton && <StyledReturnButton />}
                      </React.Fragment>
                    );
                  }}
                </Observer>
              </FieldControl>
            )}
          </Field>
          {selectionInfoFunction && (
            <Observer>
              {() => {
                const error = this.getError();
                const value = model.getValue(name);
                return (
                  ((typeof props.visible === 'function'
                    ? props.visible({...this.props, ...props} as ResolvedProps)
                    : true) &&
                    selectionInfoFunction({...props, error, value})) ||
                  null
                );
              }}
            </Observer>
          )}
          {saveOnBlurFieldOverride && (
            <Observer>
              {() => {
                const showFieldSave = model.getFieldState(name, 'showSave');
                const value = model.getValue(name);

                if (!showFieldSave) {
                  return null;
                }

                return (
                  <PanelAlert type={saveMessageAlertType}>
                    <MessageAndActions>
                      <div>
                        {typeof saveMessage === 'function'
                          ? saveMessage({...props, value})
                          : saveMessage}
                      </div>
                      <ButtonBar gap={1}>
                        <Button onClick={this.handleCancelField}>{t('Cancel')}</Button>
                        <Button
                          priority="primary"
                          type="button"
                          onClick={this.handleSaveField}
                        >
                          {t('Save')}
                        </Button>
                      </ButtonBar>
                    </MessageAndActions>
                  </PanelAlert>
                );
              }}
            </Observer>
          )}
        </React.Fragment>
      );
    };

    const observedProps = propsToObserver
      .filter(p => typeof this.props[p] === 'function')
      .map(p => [
        p,
        () => {
          const innerProps: object = this.props;
          return (innerProps as {[key: string]: ObserverReducerFn<any>})[p]({
            ...this.props,
            model,
          });
        },
      ]);

    // This field has no properties that require observation to compute their
    // value, this field is static and will not be re-rendered.
    if (observedProps.length === 0) {
      return makeField();
    }

    const reducer: any = (a, [prop, fn]) => ({...a, [prop]: fn()});
    const observedPropsFn = () => makeField(observedProps.reduce(reducer, {}));

    return <Observer>{observedPropsFn}</Observer>;
  }
}

export default FormField;

const MessageAndActions = styled('div')`
  display: grid;
  grid-template-columns: 1fr max-content;
  grid-gap: ${space(2)};
  align-items: center;
`;

const StyledReturnButton = styled(ReturnButton)`
  position: absolute;
  right: 0;
  top: 0;
`;
