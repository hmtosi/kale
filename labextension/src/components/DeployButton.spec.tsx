// Copyright 2026 The Kubeflow Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import * as React from 'react';
import {
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import { SplitDeployButton } from './DeployButton';
import type { DeployType } from '../widgets/LeftPanel';

const DEFAULT_LABEL = 'Compile and Run';

const renderButton = (
  props: { running?: boolean; disabled?: boolean } = {},
) => {
  const handleClick = jest.fn();
  render(
    <SplitDeployButton
      running={props.running ?? false}
      disabled={props.disabled ?? false}
      handleClick={handleClick}
    />,
  );
  return { handleClick };
};

/** Both halves of the split button: the labelled action and the three-dots toggle. */
const halves = () =>
  within(screen.getByRole('group', { name: 'split button' })).getAllByRole(
    'button',
  );

const mainButton = () => halves()[0];
const dropdownButton = () => halves()[1];

const openMenu = async () => {
  fireEvent.click(dropdownButton());
  return screen.findByRole('menu');
};

const selectFromMenu = async (label: string) => {
  await openMenu();
  fireEvent.click(screen.getByRole('menuitem', { name: label }));
};

describe('SplitDeployButton', () => {
  it('surfaces "Compile and Run" as the default action', () => {
    renderButton();

    expect(mainButton()).toHaveTextContent(DEFAULT_LABEL);
  });

  it('dispatches the surfaced action when the main button is clicked', () => {
    const { handleClick } = renderButton();

    fireEvent.click(mainButton());

    expect(handleClick).toHaveBeenCalledTimes(1);
    expect(handleClick).toHaveBeenCalledWith('run');
  });

  it('offers every deploy action in the menu', async () => {
    renderButton();

    const menu = await openMenu();

    expect(
      within(menu)
        .getAllByRole('menuitem')
        .map(i => i.textContent),
    ).toEqual(['Compile and Run', 'Compile and Upload', 'Compile and Save']);
  });

  describe.each<[string, DeployType]>([
    ['Compile and Run', 'run'],
    ['Compile and Upload', 'upload'],
    ['Compile and Save', 'compile'],
  ])('selecting "%s" from the menu', (label, expected) => {
    it(`applies "${expected}" immediately`, async () => {
      const { handleClick } = renderButton();

      await selectFromMenu(label);

      expect(handleClick).toHaveBeenCalledTimes(1);
      expect(handleClick).toHaveBeenCalledWith(expected);
    });

    it('surfaces it as the button label and closes the menu', async () => {
      renderButton();

      await selectFromMenu(label);

      expect(mainButton()).toHaveTextContent(label);
      await waitFor(() =>
        expect(screen.queryByRole('menu')).not.toBeInTheDocument(),
      );
    });

    it('re-applies it on a subsequent main button click', async () => {
      const { handleClick } = renderButton();

      await selectFromMenu(label);
      handleClick.mockClear();

      fireEvent.click(mainButton());

      expect(handleClick).toHaveBeenCalledTimes(1);
      expect(handleClick).toHaveBeenCalledWith(expected);
    });
  });

  it('shows a progress spinner and disables both halves while a deploy is running', () => {
    renderButton({ running: true });

    expect(mainButton()).toBeDisabled();
    expect(dropdownButton()).toBeDisabled();
    expect(mainButton()).not.toHaveTextContent(DEFAULT_LABEL);
    expect(screen.getByRole('progressbar')).toBeInTheDocument();
  });

  it('disables both halves when the pipeline metadata is invalid', () => {
    renderButton({ disabled: true });

    expect(mainButton()).toBeDisabled();
    expect(dropdownButton()).toBeDisabled();
  });

  it('does not dispatch when the main button is clicked while disabled', () => {
    const { handleClick } = renderButton({ disabled: true });

    fireEvent.click(mainButton());

    expect(handleClick).not.toHaveBeenCalled();
  });
});
